
use event::{FloEventId, ActorId, OwnedFloEvent, VersionVector};
use engine::consumer::client::context::{ConnectionContext, CursorType};
use engine::consumer::filecursor::{Cursor, CursorMessage};
use engine::api::{ConnectionId, NamespaceGlob, ConsumerFilter, ConsumerState, ClientConnect};
use protocol::{ProtocolMessage, ServerMessage, ErrorMessage, ErrorKind};
use super::connection_state::{ConnectionState};

use std::fmt::{self, Debug};
use std::net::SocketAddr;
use futures::sync::mpsc::UnboundedSender;


pub struct ClientConnection {
    connection_id: ConnectionId,
    address: SocketAddr,
    client_sender: UnboundedSender<ServerMessage>,
    state: ConnectionState,
}

impl ClientConnection {

    fn from_client_connect(conn: ClientConnect, batch_size: u64) -> ClientConnection {
        let ClientConnect {connection_id, client_addr, message_sender} = conn;
        ClientConnection {
            connection_id: connection_id,
            address: client_addr,
            client_sender: message_sender,
            state: ConnectionState::init(batch_size),
        }
    }

    pub fn message_received<C: ConnectionContext>(&mut self, message: ProtocolMessage, context: &mut C) -> Result<(), ()> {
        let state_result = match &message {
            &ProtocolMessage::SetBatchSize(batch_size) => {
                self.state.set_batch_size(batch_size as u64)
            }
            &ProtocolMessage::UpdateMarker(id) => {
                self.state.update_version_vector(id)
            }
            &ProtocolMessage::NextBatch => {
                self.next_batch(context)
            }
            &ProtocolMessage::StartConsuming(ref start) => {
                self.state.create_consumer_state(self.connection_id, start).and_then(|consumer_state| {
                    ClientConnection::create_cursor(context, consumer_state, self.client_sender.clone()).map(|cursor| {
                        self.state = ConnectionState::new(cursor);
                    })
                })
            }
            other @ _ => {
                Err(ErrorMessage {
                    op_id: 0,
                    kind: ErrorKind::InvalidConsumerState,
                    description: "Internal error in consumer connection handler".to_owned(),
                })
            }
        };

        if let Err(e) = state_result {
            warn!("Error: {:?} on connection_id: {} while handling message: {:?}", e, self.connection_id, message);
            let _ = self.send(ProtocolMessage::Error(e));
            Err(())
        } else {
            Ok(())
        }
    }

    fn next_batch<C: ConnectionContext>(&mut self, context: &mut C) -> Result<(), ErrorMessage> {
        let mut new_state: Option<ConnectionState> = None;

        match self.state {
            ConnectionState::FileCursor(ref mut cursor) => {
                cursor.continue_batch().map_err(|()| {
                    ErrorMessage {
                        op_id: 0,
                        kind: ErrorKind::StorageEngineError,
                        description: "Cursor has died".to_owned()
                    }
                })?;
            }
            ConnectionState::InMemoryConsumer(ref state) => {
                let new_cursor = ClientConnection::create_cursor(context, state.clone(), self.client_sender.clone())?;
                new_state = Some(ConnectionState::new(new_cursor));
            }
            ConnectionState::NotConsuming(_) => {
                return Err(ErrorMessage {
                    op_id: 0,
                    kind: ErrorKind::InvalidConsumerState,
                    description: "Cannot advance batch because consumer is in NotConsuming state".to_owned()
                });
            }
        }

        if let Some(state) = new_state {
            self.set_state(state);
        }
        Ok(())
    }

    fn create_cursor<C: ConnectionContext>(context: &mut C, consumer: ConsumerState, sender: UnboundedSender<ServerMessage>) -> Result<CursorType, ErrorMessage> {
        context.start_consuming(consumer, sender).map_err(|err| {
            ErrorMessage {
                op_id: 0,
                kind: ErrorKind::StorageEngineError,
                description: err
            }
        })
    }

    fn set_state(&mut self, new_state: ConnectionState) {
        trace!("connection_id: {} replacing old state: {:?} with new state: {:?}", self.connection_id, self.state, new_state);
        self.state = new_state;
    }

    fn send(&mut self, message: ProtocolMessage) -> Result<(), ()> {
        self.client_sender.send(ServerMessage::Other(message)).map_err(|_| ())
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use event::{FloEventId, OwnedFloEvent, VersionVector};
    use engine::consumer::client::context::{ConnectionContext, CursorType};
    use engine::consumer::client::connection_state::{IdleState, ConnectionState};
    use engine::consumer::filecursor::{Cursor, CursorMessage};
    use engine::api::{ConnectionId, NamespaceGlob, ConsumerFilter, ConsumerState, ClientConnect};
    use protocol::{ProtocolMessage, ServerMessage, ConsumerStart};

    use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
    use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};

    #[test]
    fn continue_batch_continues_the_batch() {
        let mut subject = TestConnection::with_state(ConnectionState::init(999));

        let mut ctx = MockContext::new();
        let start_message = ConsumerStart{
            max_events: 3,
            namespace: "/foo".to_owned(),
        };
        subject.connection.message_received(ProtocolMessage::StartConsuming(start_message), &mut ctx).unwrap();
    }

    #[test]
    fn start_consuming_creates_consumer_with_default_options() {
        let mut subject = TestConnection::with_state(ConnectionState::init(999));

        let mut ctx = MockContext::new();
        let start_message = ConsumerStart{
            max_events: 3,
            namespace: "/foo".to_owned(),
        };
        subject.connection.message_received(ProtocolMessage::StartConsuming(start_message), &mut ctx).unwrap();

        let (state, _) = ctx.file_cursor_args.unwrap();
        assert_eq!(ConsumerFilter::Namespace(NamespaceGlob::new("/foo").unwrap()), state.filter);
    }

    #[test]
    fn update_marker_sets_id_in_version_vec_when_connection_is_not_consuming() {
        let mut subject = TestConnection::with_state(ConnectionState::init(999));
        let id = FloEventId::new(8, 9);
        subject.connection.message_received(ProtocolMessage::UpdateMarker(id), &mut MockContext::new());

        expect_not_consuming_state(&subject.connection, |idle_state| {
            assert_eq!(9, idle_state.version_vector.get(8));
        });
    }

    #[test]
    fn set_batch_size_updates_batch_size_when_connection_is_not_consuming() {
        let mut subject = TestConnection::with_state(ConnectionState::init(999));

        subject.connection.message_received(ProtocolMessage::SetBatchSize(888), &mut MockContext::new());

        expect_not_consuming_state(&subject.connection, |idle_state| {
            assert_eq!(888, idle_state.batch_size);
        });
    }

    #[test]
    fn connection_is_created_from_client_connect() {
        let (tx, rx) = unbounded();
        let input = ClientConnect{
            connection_id: CONNECTION_ID,
            client_addr: address(),
            message_sender: tx,
        };

        let result = ClientConnection::from_client_connect(input, BATCH_SIZE);
        assert_eq!(CONNECTION_ID, result.connection_id);
        assert_eq!(address(), result.address);

        let expected_state = ConnectionState::NotConsuming(IdleState{
            version_vector: VersionVector::new(),
            batch_size: BATCH_SIZE
        });
        assert_eq!(expected_state, result.state);
    }

    fn expect_not_consuming_state<F>(conn: &ClientConnection, fun: F) where F: Fn(&IdleState) {
        match &conn.state {
            &ConnectionState::NotConsuming(ref idle) => fun(&idle),
            other @ _ => {
                panic!("expected NotConsuming state but got: {:?}", other);
            }
        }
    }

    const CONNECTION_ID: ConnectionId = 123;
    const BATCH_SIZE: u64 = 3;
    fn address() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6789))
    }

    struct TestConnection {
        connection: ClientConnection,
        receiver: UnboundedReceiver<ServerMessage>,
    }

    impl TestConnection {
        fn with_state(state: ConnectionState) -> TestConnection {
            let (tx, rx) = unbounded();
            let conn = ClientConnection {
                connection_id: CONNECTION_ID,
                address: address(),
                client_sender: tx,
                state: state,
            };
            TestConnection {
                connection: conn,
                receiver: rx
            }
        }
    }

    struct MockContext {
        file_cursor_return_val: Option<Result<CursorType, String>>,
        file_cursor_args: Option<(ConsumerState, UnboundedSender<ServerMessage>)>,
    }

    impl ConnectionContext for MockContext {
        fn start_consuming(&mut self, consumer_state: ConsumerState, client_sender: UnboundedSender<ServerMessage>) -> Result<CursorType, String> {
            let ret_val = self.file_cursor_return_val.take().unwrap();
            self.file_cursor_args = Some((consumer_state, client_sender));
            ret_val
        }
    }

    impl MockContext {
        fn new() -> MockContext {
            MockContext {
                file_cursor_return_val: Some(Ok(CursorType::File(Box::new(MockCursor::new())))),
                file_cursor_args: None
            }
        }
    }

    #[derive(Debug)]
    struct MockCursor {
        send_args: Vec<CursorMessage>,
    }

    impl MockCursor {
        fn new() -> MockCursor {
            MockCursor {
                send_args: Vec::new()
            }
        }
    }

    impl Cursor for MockCursor {
        fn send(&mut self, message: CursorMessage) -> Result<(), ()> {
            self.send_args.push(message);
            Ok(())
        }
        fn get_thread_name(&self) -> &str {
            "mock cursor thread name"
        }
    }
}