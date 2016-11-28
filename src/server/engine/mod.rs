pub mod api;

mod client;

pub use self::client::{Client, ClientSendError};

use self::api::{ConnectionId, ServerMessage, ClientMessage, ClientConnect, ProduceEvent, EventAck};
use event_store::StorageEngine;
use flo_event::{ActorId, OwnedFloEvent, EventCounter, FloEventId};

use futures::sync::mpsc::UnboundedSender;

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::path::PathBuf;
use std::net::SocketAddr;

pub fn run(_storage_dir: PathBuf) -> mpsc::Sender<ClientMessage> {
    let (sender, receiver) = mpsc::channel::<ClientMessage>();

    //TODO: write this whole fucking thing
    thread::spawn(move || {

        loop {
            match receiver.recv() {
                Ok(msg) => info!("Received message: {:?}", msg),
                Err(recv_err) => {
                    error!("Receive Error: {:?}", recv_err);
                    break;
                }
            }
        }
    });

    sender
}

pub struct Engine<S: StorageEngine, C: Client> {
    actor_id: ActorId,
    event_store: S,
    client_map: HashMap<ConnectionId, C>,
}

impl <S: StorageEngine, C: Client> Engine<S, C> {

    pub fn new(event_store: S, actor_id: ActorId) -> Engine<S, C> {
        Engine {
            actor_id: actor_id,
            event_store: event_store,
            client_map: HashMap::new(),
        }
    }

    pub fn process(&mut self, client_message: ClientMessage) -> Result<(), String> {

        match client_message {
            ClientMessage::ClientConnect(client_connect) => {
                self.client_connect(client_connect);
                Ok(())
            }
            ClientMessage::Produce(produce_event) => {
                self.produce_event(produce_event)
            }
            msg @ _ => Err(format!("Haven't implemented handling for client message: {:?}", msg))
        }
    }

    fn produce_event(&mut self, event: ProduceEvent) -> Result<(), String> {
        let connection_id = event.connection_id;
        let op_id = event.op_id;
        let event_id = FloEventId::new(self.actor_id, 1);
        let owned_event = OwnedFloEvent {
            id: event_id,
            namespace: "whatever".to_owned(),
            data: event.event_data,
        };

        self.event_store.store(owned_event).map(|()| {
            self.send_to_client(connection_id, ServerMessage::EventPersisted(EventAck{
                op_id: op_id,
                event_id: event_id,
            }))
        });
        Ok(())
    }

    fn client_connect(&mut self, client_connect: ClientConnect) {
        let client = C::from_client_connect(client_connect);
        let id = client.connection_id();
        self.client_map.insert(id, client);
    }

    fn send_to_client(&mut self, connection_id: ConnectionId, message: ServerMessage) -> Result<(), String> {
        let mut remove_client = false;
        let result = self.client_map.get_mut(&connection_id)
                .ok_or_else(|| {format!("Connection: {} did not exist in the map", connection_id)})
                .and_then(|client| {
                    if let Err(_) = client.send(message) {
                        error!("Error sending message to client: {}, removing it from the map", connection_id);
                        remove_client = true;
                    }
                    // The client was in the map, so even a send error is OK, since we don't necessarily want to shut down the server
                    Ok(())
                });

        if remove_client {
            self.client_map.remove(&connection_id);
        }
        result
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use flo_event::{FloEvent, FloEventId, ActorId, EventCounter, OwnedFloEvent};
    use super::api::{ConnectionId, ClientMessage, ClientConnect, ProduceEvent, ServerMessage, EventAck};
    use std::str::FromStr;

    const SUBJECT_ACTOR_ID: ActorId = 123;
    type TestEngine = Engine<Vec<OwnedFloEvent>, MockClient>;

    fn client_addr()-> ::std::net::SocketAddr {
        ::std::net::SocketAddr::from_str("127.0.0.1:12345").unwrap()
    }

    fn assert_events_stored(actual: Vec<OwnedFloEvent>, expected_data: &[&[u8]]) {
        let act: Vec<&[u8]> = actual.iter().map(|evt| evt.data()).collect();
        assert_eq!(&act[..], expected_data);
    }

    #[test]
    fn engine_saves_event_and_sends_ack() {
        let connection: ConnectionId = 123;
        let mut subject = subject_with_connected_clients(&[connection]);

        let mut input = ProduceEvent{
            connection_id: 123,
            op_id: 1,
            event_data: b"the event data".to_vec(),
        };
        subject.process(ClientMessage::Produce(input)).unwrap();

        assert_events_stored(subject.event_store, &[b"the event data"]);

        let client = subject.client_map.get(&connection).unwrap();
        client.assert_messages_sent(&[
            ServerMessage::EventPersisted(EventAck{op_id: 1, event_id: FloEventId::new(SUBJECT_ACTOR_ID, 1)})
        ]);
    }

    struct MockClient {
        connection_id: ConnectionId,
        addr: ::std::net::SocketAddr,
        sent_messages: Vec<ServerMessage>,
    }

    impl MockClient {

        fn assert_messages_sent(&self, messages: &[ServerMessage]) {
            assert_eq!(messages, &self.sent_messages[..]);
        }
    }

    impl Client for MockClient {
        fn from_client_connect(message: ClientConnect) -> Self {
            MockClient {
                connection_id: message.connection_id,
                addr: message.client_addr,
                sent_messages: Vec::new(),
            }
        }

        fn connection_id(&self) -> ConnectionId {
            self.connection_id
        }

        fn addr(&self) -> &::std::net::SocketAddr {
            &self.addr
        }

        fn send(&mut self, message: ServerMessage) -> Result<(), ClientSendError> {
            self.sent_messages.push(message);
            Ok(())
        }
    }

    fn subject_with_connected_clients(clients: &[ConnectionId]) -> Engine<Vec<OwnedFloEvent>, MockClient> {
        let mut subject = subject();

        for &client in clients {
            subject.client_map.insert(client, MockClient{
                connection_id: client,
                addr: client_addr(),
                sent_messages: Vec::new(),
            });
        }
        subject
    }

    fn subject() -> Engine<Vec<OwnedFloEvent>, MockClient> {
        Engine::<Vec<OwnedFloEvent>, MockClient>::new(Vec::new(), SUBJECT_ACTOR_ID)
    }

}

