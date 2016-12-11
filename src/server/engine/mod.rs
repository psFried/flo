pub mod api;

mod client;

pub use self::client::{Client, ClientManager, ClientManagerImpl, ClientSendError};

use self::api::{ConnectionId, ServerMessage, ClientMessage, ClientConnect, ProduceEvent, EventAck};
use event_store::StorageEngine;
use flo_event::{ActorId, OwnedFloEvent, EventCounter, FloEventId};

use futures::sync::mpsc::UnboundedSender;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, mpsc};
use std::thread;
use std::path::PathBuf;
use std::net::SocketAddr;

pub fn run(_storage_dir: PathBuf) -> mpsc::Sender<ClientMessage> {
    let (sender, receiver) = mpsc::channel::<ClientMessage>();

    //TODO: write this whole fucking thing
    thread::spawn(move || {
        let mut engine = Engine::new(Vec::<OwnedFloEvent>::new(), ClientManagerImpl::new(), 1);

        loop {
            match receiver.recv() {
                Ok(msg) => {
                    engine.process(msg).unwrap();
                }
                Err(recv_err) => {
                    error!("Receive Error: {:?}", recv_err);
                    break;
                }
            }
        }
    });

    sender
}

pub struct Engine<S: StorageEngine, C: ClientManager> {
    actor_id: ActorId,
    event_store: S,
    client_manager: C,
    highest_event_id: EventCounter,
}

impl <S: StorageEngine, C: ClientManager> Engine<S, C> {

    pub fn new(event_store: S, client_manager: C, actor_id: ActorId) -> Engine<S, C> {
        Engine {
            actor_id: actor_id,
            event_store: event_store,
            client_manager: client_manager,
            highest_event_id: 1,
        }
    }

    pub fn process(&mut self, client_message: ClientMessage) -> Result<(), String> {
        match client_message {
            ClientMessage::ClientConnect(client_connect) => {
                self.client_manager.add_connection(client_connect);
                Ok(())
            }
            ClientMessage::Produce(produce_event) => {
                self.produce_event(produce_event)
            }
            msg @ _ => Err(format!("Haven't implemented handling for client message: {:?}", msg))
        }
    }

    fn produce_event(&mut self, event: ProduceEvent) -> Result<(), String> {
        let producer_id = event.connection_id;
        let op_id = event.op_id;
        let event_id = FloEventId::new(self.actor_id, self.highest_event_id);
        let owned_event = OwnedFloEvent {
            id: event_id,
            namespace: "whatever".to_owned(),
            data: event.event_data,
        };

        self.event_store.store(&owned_event).map(|()| {
            self.highest_event_id += 1;
            debug!("Stored event, new highest_event_id: {}", self.highest_event_id);

            self.client_manager.send_message(producer_id, ServerMessage::EventPersisted(EventAck{
                op_id: op_id,
                event_id: event_id,
            }));

            self.client_manager.send_event(producer_id, owned_event);
        });
        Ok(())
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use flo_event::{FloEvent, FloEventId, ActorId, EventCounter, OwnedFloEvent};
    use super::api::{ConnectionId, ClientMessage, ClientConnect, ProduceEvent, ServerMessage, EventAck};
    use std::str::FromStr;
    use std::sync::Arc;
    use std::collections::{HashMap};

    const SUBJECT_ACTOR_ID: ActorId = 123;
    type TestEngine = Engine<Vec<OwnedFloEvent>, MockClientManager>;

    #[test]
    fn engine_saves_event_and_sends_ack() {
        let connection: ConnectionId = 123;
        let mut subject = subject();

        let mut input = ProduceEvent{
            connection_id: 123,
            op_id: 1,
            event_data: b"the event data".to_vec(),
        };
        subject.process(ClientMessage::Produce(input)).unwrap();

        assert_events_stored(subject.event_store, &[b"the event data"]);

        subject.client_manager.assert_messages_sent(connection, &[
            ServerMessage::EventPersisted(EventAck{op_id: 1, event_id: FloEventId::new(SUBJECT_ACTOR_ID, 1)})
        ]);
    }

    #[test]
    fn engine_increments_event_counter_for_each_event_saved() {
        let connection: ConnectionId = 123;
        let mut subject = subject();

        subject.process(ClientMessage::Produce(ProduceEvent{
            connection_id: 123,
            op_id: 1,
            event_data: b"one".to_vec(),
        })).unwrap();
        subject.process(ClientMessage::Produce(ProduceEvent{
            connection_id: 123,
            op_id: 1,
            event_data: b"two".to_vec(),
        })).unwrap();
        subject.process(ClientMessage::Produce(ProduceEvent{
            connection_id: 123,
            op_id: 1,
            event_data: b"three".to_vec(),
        })).unwrap();

        assert_events_stored(subject.event_store, &[b"one", b"two", b"three"]);


        subject.client_manager.assert_messages_sent(connection, &[
            ServerMessage::EventPersisted(EventAck{op_id: 1, event_id: FloEventId::new(SUBJECT_ACTOR_ID, 1)}),
            ServerMessage::EventPersisted(EventAck{op_id: 1, event_id: FloEventId::new(SUBJECT_ACTOR_ID, 2)}),
            ServerMessage::EventPersisted(EventAck{op_id: 1, event_id: FloEventId::new(SUBJECT_ACTOR_ID, 3)})
        ]);
    }

    #[test]
    fn event_is_sent_to_other_connected_clients() {
        let producer: ConnectionId = 123;
        let consumer1: ConnectionId = 234;
        let consumer2: ConnectionId = 234;

        let mut subject = subject();

        subject.process(ClientMessage::Produce(ProduceEvent{
            connection_id: 123,
            op_id: 1,
            event_data: b"event data".to_vec(),
        })).unwrap();

        let expected = &[&b"event data"[..]];

        subject.client_manager.assert_events_received(123, expected);
    }

    struct MockClientManager {
        sent_messages: HashMap<ConnectionId, Vec<ServerMessage>>,
        events_produced: Vec<(ConnectionId, OwnedFloEvent)>,
    }

    impl MockClientManager {
        fn new() -> MockClientManager {
            MockClientManager {
                sent_messages: HashMap::new(),
                events_produced: Vec::new(),
            }
        }
        fn assert_events_received(&self, connection_id: ConnectionId, expected_data: &[&[u8]]) {
            let actual = self.events_produced.iter().filter(|e| {
                e.0 == connection_id
            }).map(|e| {
                e.1.data()
            }).collect::<Vec<&[u8]>>();

            assert_eq!(expected_data, &actual[..]);
        }

        fn assert_messages_sent(&self, connection_id: ConnectionId, messages: &[ServerMessage]) {
            let actual = self.sent_messages.get(&connection_id)
                    .expect(&format!("No messages sent to client: {}, expected: {}", connection_id, messages.len()));
            assert_eq!(messages, &actual[..]);
        }
    }

    impl ClientManager for MockClientManager {
        fn add_connection(&mut self, client_connect: ClientConnect) {
            //no-op
        }

        fn send_event(&mut self, event_producer: ConnectionId, event: OwnedFloEvent) {
            self.events_produced.push((event_producer, event));
        }

        fn send_message(&mut self, recipient: ConnectionId, message: ServerMessage) -> Result<(), ClientSendError> {
            self.sent_messages.entry(recipient).or_insert_with(|| {Vec::new()}).push(message);
            Ok(())
        }
    }

    fn subject() -> TestEngine {
        TestEngine::new(Vec::new(), MockClientManager::new(), SUBJECT_ACTOR_ID)
    }

    fn client_addr()-> ::std::net::SocketAddr {
        ::std::net::SocketAddr::from_str("127.0.0.1:12345").unwrap()
    }

    fn assert_events_stored(actual: Vec<OwnedFloEvent>, expected_data: &[&[u8]]) {
        let act: Vec<&[u8]> = actual.iter().map(|evt| evt.data()).collect();
        assert_eq!(&act[..], expected_data);
    }
}

