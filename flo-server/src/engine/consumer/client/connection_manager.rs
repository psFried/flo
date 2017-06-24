
use event::{FloEventId, ActorId, OwnedFloEvent, VersionVector};
use engine::consumer::client::context::{ConnectionContext, CursorType};
use engine::consumer::filecursor::{Cursor, CursorMessage};
use engine::api::{ConnectionId, NamespaceGlob, ConsumerFilter, ConsumerState, ClientConnect};
use protocol::{ProtocolMessage, ServerMessage, ErrorMessage, ErrorKind, CursorInfo, ConsumerStart};
use super::connection_state::{ConnectionState};

use std::sync::Arc;
use std::fmt::{self, Debug};
use std::net::SocketAddr;
use futures::sync::mpsc::UnboundedSender;
use channels::Sender;


pub struct ClientConnection<S: Sender<ServerMessage> + 'static> {
    pub connection_id: ConnectionId,
    pub address: SocketAddr,
    client_sender: S,
    state: ConnectionState,
}

impl <S: Sender<ServerMessage> + 'static> ClientConnection<S> {

    pub fn new(connection_id: ConnectionId, addr: SocketAddr, sender: S, batch_size: u64) -> ClientConnection<S> {
        ClientConnection {
            connection_id: connection_id,
            address: addr,
            client_sender: sender,
            state: ConnectionState::init(batch_size),
        }
    }

    pub fn from_client_connect(conn: ClientConnect, batch_size: u64) -> ClientConnection<UnboundedSender<ServerMessage>> {
        let ClientConnect {connection_id, client_addr, message_sender} = conn;
        ClientConnection::new(connection_id, client_addr, message_sender, batch_size)
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
                self.start_consuming(start, context)
            }
            &ProtocolMessage::StopConsuming => {
                self.state.stop_consuming()
            }
            &ProtocolMessage::AckEvent(ref ack) => {
                trace!("received AckEvent from connection_id: {}: {:?}", self.connection_id, ack);
                Ok(())
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

    pub fn maybe_send_event(&mut self, event: &Arc<OwnedFloEvent>) -> Result<(), ()> {
        let should_send = {
            let event_ref = event.as_ref();
            self.state.should_send_event(event_ref)
        };
        if should_send {
            let id = event.id;
            let result = self.client_sender.send(ServerMessage::Event(event.clone())).map_err(|_| ());
            if result.is_ok() {
                debug!("Sent event: {} to connection_id: {}", id, self.connection_id);
                self.state.event_sent(id);
            }
            result
        } else {
            Ok(())
        }
    }

    pub fn continue_cursor<C: ConnectionContext>(&mut self, last_state: ConsumerState, context: &mut C) -> Result<(), ()> {
        let batch_size = last_state.batch_size;
        let cursor_result = ClientConnection::create_cursor(context, last_state, &self.client_sender);

        if let Err(ref err) = cursor_result {
            error!("Error continuing cursor for connection_id: {}, err: {:?}", self.connection_id, err);
            let new_state = ConnectionState::init(batch_size);
            self.set_state(new_state);
        }

        let result = cursor_result.map(|cursor_type| {
            match cursor_type {
                CursorType::File(file_cursor) => {
                    self.set_state(ConnectionState::FileCursor(file_cursor, batch_size));
                }
                CursorType::InMemory(new_state) => {
                    self.set_state(ConnectionState::InMemoryConsumer(new_state));
                }
            }
        });

        if let Err(err_message) = result {
            self.client_sender.send(ServerMessage::Other(ProtocolMessage::Error(err_message))).map_err(|send_err| {
                // map to ()
                warn!("Unable to send error message to connection_id: {}, message: {:?}", self.connection_id, send_err);
            })
        } else {
            Ok(())
        }
    }


    fn start_consuming<C: ConnectionContext>(&mut self, start: &ConsumerStart, context: &mut C) -> Result<(), ErrorMessage> {
        self.state.create_consumer_state(self.connection_id, start).and_then(|consumer_state| {
            let batch_size = consumer_state.batch_size;

            let start_message = ProtocolMessage::CursorCreated(CursorInfo{
                batch_size: batch_size as u32
            });

            self.client_sender.send(ServerMessage::Other(start_message)).map_err(|_| {
                ErrorMessage{
                    op_id: 0,
                    kind: ErrorKind::InvalidConsumerState,
                    description: "Error sending start message to client".to_owned(),
                }
            })?; // early return if sending the start message fails

            ClientConnection::create_cursor(context, consumer_state, &self.client_sender).map(|cursor| {
                self.state = ConnectionState::new(cursor, batch_size);
            })
        })
    }

    fn next_batch<C: ConnectionContext>(&mut self, context: &mut C) -> Result<(), ErrorMessage> {
        let mut new_state: Option<ConnectionState> = None;

        debug!("processing next batch for connection_id: {}", self.connection_id);

        match self.state {
            ConnectionState::FileCursor(ref mut cursor, _) => {
                cursor.continue_batch().map_err(|()| {
                    ErrorMessage {
                        op_id: 0,
                        kind: ErrorKind::StorageEngineError,
                        description: "Cursor has died".to_owned()
                    }
                })?;
            }
            ConnectionState::InMemoryConsumer(ref mut state) => {
                let batch_size = state.batch_size;
                state.start_new_batch();
                let new_cursor = ClientConnection::create_cursor(context, state.clone(), &self.client_sender)?;
                new_state = Some(ConnectionState::new(new_cursor, batch_size));
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

    fn create_cursor<C: ConnectionContext>(context: &mut C, consumer: ConsumerState, sender: &S) -> Result<CursorType, ErrorMessage> {
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
    use event::{FloEventId, OwnedFloEvent, VersionVector, Timestamp};
    use event::time::from_millis_since_epoch;
    use engine::consumer::client::context::{ConnectionContext, CursorType};
    use engine::consumer::client::connection_state::{IdleState, ConnectionState};
    use engine::consumer::filecursor::{Cursor, CursorMessage};
    use engine::api::{ConnectionId, NamespaceGlob, ConsumerFilter, ConsumerState, ClientConnect};
    use protocol::{ProtocolMessage, ServerMessage, ConsumerStart};

    use channels::{Sender, MockSender};
    use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
    use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};

    #[test]
    fn maybe_send_event_sends_only_events_matching_filter() {
        let namespace = "/foo".to_owned();
        let mut subject = subject_with_state(ConnectionState::init(999));

        let mut ctx = InMemoryMockContext::new();
        let start_message = ConsumerStart{
            max_events: 3,
            namespace: namespace.clone(),
        };
        subject.message_received(ProtocolMessage::StartConsuming(start_message), &mut ctx).unwrap();

        let expected_state = ConsumerState::new(CONNECTION_ID,
                                                VersionVector::new(),
                                                ConsumerFilter::Namespace(NamespaceGlob::new(&namespace).unwrap()),
                                                999);

        ctx.assert_argument_received(expected_state);
        subject.client_sender.assert_message_sent(ServerMessage::Other(ProtocolMessage::CursorCreated(CursorInfo{batch_size: 999})));

        let data = "data for event".to_owned().into_bytes();
        let timestamp = from_millis_since_epoch(678910);
        let expected_event = Arc::new(OwnedFloEvent::new(FloEventId::new(1, 7), None, timestamp, namespace.clone(), data.clone()));
        let extra_event = Arc::new(OwnedFloEvent::new(FloEventId::new(1, 8), None, timestamp, "/bar".to_owned(), data));

        subject.maybe_send_event(&expected_event).unwrap();
        subject.maybe_send_event(&extra_event).unwrap();

        subject.client_sender.assert_message_sent(ServerMessage::Event(expected_event));
        subject.client_sender.assert_no_more_messages_sent();
    }

    #[test]
    fn continue_batch_continues_the_batch() {
        let namespace = "/foo".to_owned();
        let mut subject = subject_with_state(ConnectionState::init(888888));

        let mut ctx = InMemoryMockContext::new();
        subject.message_received(ProtocolMessage::SetBatchSize(5), &mut ctx).unwrap();

        let start_message = ConsumerStart{
            max_events: 3, //TODO: remove max_events field
            namespace: namespace.clone(),
        };
        subject.message_received(ProtocolMessage::StartConsuming(start_message), &mut ctx).unwrap();
        let expected_state = ConsumerState::new(CONNECTION_ID,
                                                VersionVector::new(),
                                                ConsumerFilter::Namespace(NamespaceGlob::new(&namespace).unwrap()),
                                                5);

        ctx.assert_argument_received(expected_state);
        subject.client_sender.assert_message_sent(ServerMessage::Other(ProtocolMessage::CursorCreated(CursorInfo{batch_size: 5})));

        let data = "data for event".to_owned().into_bytes();
        let timestamp = from_millis_since_epoch(678910);
        for i in 1..6 {
            let id = FloEventId::new(1, i);
            let expected_event = Arc::new(OwnedFloEvent::new(id, None, timestamp, namespace.clone(), data.clone()));
            subject.maybe_send_event(&expected_event).unwrap();
            subject.client_sender.assert_message_sent(ServerMessage::Event(expected_event));
        }

        let extra_event = Arc::new(OwnedFloEvent::new(FloEventId::new(1, 7), None, timestamp, namespace.clone(), data.clone()));
        subject.maybe_send_event(&extra_event).unwrap();
        subject.client_sender.assert_no_more_messages_sent();

        let mut ctx = InMemoryMockContext::new();
        subject.message_received(ProtocolMessage::NextBatch, &mut ctx).unwrap();

        let mut expected_version_vec = VersionVector::new();
        expected_version_vec.set(FloEventId::new(1, 5));
        let expected_state = ConsumerState::new(CONNECTION_ID,
                                                expected_version_vec,
                                                ConsumerFilter::Namespace(NamespaceGlob::new(&namespace).unwrap()),
                                                5);
        ctx.assert_argument_received(expected_state);

        subject.maybe_send_event(&extra_event).unwrap();

        subject.client_sender.assert_message_sent(ServerMessage::Event(extra_event));
    }


    #[test]
    fn start_consuming_creates_consumer_with_default_options() {
        let mut subject = subject_with_state(ConnectionState::init(999));

        let mut ctx = MockContext::returning_file_cursor();
        let start_message = ConsumerStart{
            max_events: 3,
            namespace: "/foo".to_owned(),
        };
        subject.message_received(ProtocolMessage::StartConsuming(start_message), &mut ctx).unwrap();

        let state = ctx.start_consuming_args.unwrap();
        assert_eq!(ConsumerFilter::Namespace(NamespaceGlob::new("/foo").unwrap()), state.filter);
    }

    #[test]
    fn update_marker_sets_id_in_version_vec_when_connection_is_not_consuming() {
        let mut subject = subject_with_state(ConnectionState::init(999));
        let id = FloEventId::new(8, 9);
        subject.message_received(ProtocolMessage::UpdateMarker(id), &mut MockContext::returning_file_cursor());

        expect_not_consuming_state(&subject, |idle_state| {
            assert_eq!(9, idle_state.version_vector.get(8));
        });
    }

    #[test]
    fn set_batch_size_updates_batch_size_when_connection_is_not_consuming() {
        let mut subject = subject_with_state(ConnectionState::init(999));

        subject.message_received(ProtocolMessage::SetBatchSize(888), &mut MockContext::returning_file_cursor());

        expect_not_consuming_state(&subject, |idle_state| {
            assert_eq!(888, idle_state.batch_size);
        });
    }

    fn expect_not_consuming_state<F>(conn: &ClientConnection<MockSender<ServerMessage>>, fun: F) where F: Fn(&IdleState) {
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

    type TestConnection = ClientConnection<MockSender<ServerMessage>>;

    fn subject_with_state(state: ConnectionState) -> TestConnection {
        ClientConnection {
            connection_id: CONNECTION_ID,
            address: address(),
            client_sender: MockSender::new(),
            state: state,
        }
    }


    fn assert_events_sent_in_order(subject: &TestConnection, expected: Vec<Arc<OwnedFloEvent>>) {
        let mut count = 0;
        let expected_count = expected.len();

        for expected_event in expected {

        }
    }

    struct MockContext {
        start_consuming_return_val: Option<Result<CursorType, String>>,
        start_consuming_args: Option<ConsumerState>,
    }

    impl ConnectionContext for MockContext {
        fn start_consuming<S: Sender<ServerMessage>>(&mut self, consumer_state: ConsumerState, client_sender: &S) -> Result<CursorType, String> {
            let ret_val = self.start_consuming_return_val.take().unwrap();
            self.start_consuming_args = Some(consumer_state);
            ret_val
        }
    }

    impl MockContext {
        fn returning_file_cursor() -> MockContext {
            MockContext {
                start_consuming_return_val: Some(Ok(CursorType::File(Box::new(MockCursor::new())))),
                start_consuming_args: None
            }
        }

        fn assert_argument_received(&self, expected: ConsumerState) {
            assert_eq!(Some(expected), self.start_consuming_args)
        }
    }

    struct InMemoryMockContext {
        arg: Option<ConsumerState>
    }

    impl ConnectionContext for InMemoryMockContext {
        fn start_consuming<S: Sender<ServerMessage> + 'static>(&mut self, consumer_state: ConsumerState, client_sender: &S) -> Result<CursorType, String> {
            assert!(self.arg.is_none(), "start_consuming has already been called");
            self.arg = Some(consumer_state.clone());
            Ok(CursorType::InMemory(consumer_state))
        }
    }

    impl InMemoryMockContext {
        fn new() -> InMemoryMockContext {
            InMemoryMockContext {
                arg: None
            }
        }
        fn assert_argument_received(&self, expected: ConsumerState) {
            assert_eq!(Some(expected), self.arg);
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
