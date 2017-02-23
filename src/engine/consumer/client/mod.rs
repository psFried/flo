mod namespace;

pub use self::namespace::NamespaceGlob;

use event::{FloEvent, FloEventId, OwnedFloEvent, ActorId, EventCounter};
use engine::api::{ConnectionId, ClientConnect};
use protocol::{ServerMessage, ProtocolMessage};
use channels::{Sender, SendError};
use engine::version_vec::VersionVector;

use futures::sync::mpsc::UnboundedSender;

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;


static SEND_ERROR_DESC: &'static str = "Failed to send message through Client Channel";

#[derive(Debug, PartialEq)]
pub struct ClientSendError(pub ServerMessage);

impl ::std::error::Error for ClientSendError {
    fn description(&self) -> &str {
        SEND_ERROR_DESC
    }
}

impl ::std::fmt::Display for ClientSendError {
    fn fmt(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(formatter, "{}", SEND_ERROR_DESC)
    }
}

#[derive(Debug, PartialEq)]
pub enum ConsumeType {
    File,
    Memory,
}

pub struct ConsumingState {
    pub last_event_id: FloEventId,
    pub consume_type: ConsumeType,
    pub remaining: u64,
    pub namespace: NamespaceGlob,
}

impl ConsumingState {
    pub fn forward_from_file(id: FloEventId, namespace: NamespaceGlob, limit: u64) -> ConsumingState {
        ConsumingState {
            last_event_id: id,
            consume_type: ConsumeType::File,
            remaining: limit,
            namespace: namespace
        }
    }

    pub fn forward_from_memory(id: FloEventId, namespace: NamespaceGlob, limit: u64) -> ConsumingState {
        ConsumingState {
            namespace: namespace,
            last_event_id: id,
            consume_type: ConsumeType::Memory,
            remaining: limit,
        }
    }
}

pub enum ClientState {
    NotConsuming(FloEventId),
    Consuming(ConsumingState),
}

impl ClientState {
    fn update_event_sent(&mut self, id: FloEventId, connection_id: ConnectionId) {
        let mut new_state: Option<ClientState> = None;

        match self {
            &mut ClientState::Consuming(ref state) if state.remaining == 1 => {
                // this is the last event, so transition to NotConsuming
                new_state = Some(ClientState::NotConsuming(state.last_event_id));
            }
            &mut ClientState::Consuming(ref mut state) => {
                trace!("updating connection_id: {}, last_event_id: {:?}, remaining: {}", connection_id, state.last_event_id, state.remaining);
                state.last_event_id = id;
                state.remaining -= 1;
            }
            &mut ClientState::NotConsuming(_) => {
                unreachable!() //TODO: maybe redesign this to provide a better guarantee that no event will be sent while in NotConsuming state
            }
        }

        if let Some(state) = new_state {
            *self = state;
        }
    }
}

pub type ClientImpl = Client<UnboundedSender<ServerMessage>>;

#[derive(Debug)]
enum NewConsumerState {
    Consumer{
        namespace: NamespaceGlob,
        remaining_events: u64,
    },
    Peer,
    NotConsuming,
}

impl NewConsumerState {
    fn is_not_consuming(&self) -> bool {
        if let NewConsumerState::NotConsuming = *self {
            true
        } else {
            false
        }
    }
}

pub struct Client<T: Sender<ServerMessage>> {
    pub connection_id: ConnectionId,
    pub addr: SocketAddr,
    sender: T,
    version_vector: VersionVector,
    new_consumer_state: NewConsumerState,
    // TODO: deprecate this field
    consumer_state: ClientState,
}

impl Client<UnboundedSender<ServerMessage>> {
    pub fn from_client_connect(connect_message: ClientConnect) -> Client<UnboundedSender<ServerMessage>> {
        Client {
            connection_id: connect_message.connection_id,
            addr: connect_message.client_addr,
            sender: connect_message.message_sender,
            version_vector: VersionVector::new(),
            new_consumer_state: NewConsumerState::NotConsuming,

            consumer_state: ClientState::NotConsuming(FloEventId::new(0, 0)),
        }
    }
}

impl <T: Sender<ServerMessage>> Client<T> {

    pub fn new(connection_id: ConnectionId, addr: SocketAddr, sender: T) -> Client<T> {
        Client {
            connection_id: connection_id,
            addr: addr,
            sender: sender,
            version_vector: VersionVector::new(),
            new_consumer_state: NewConsumerState::NotConsuming,

            consumer_state: ClientState::NotConsuming(FloEventId::zero())
        }
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn should_send_event<E: FloEvent>(&self, event: &E) -> bool {
        let current_id = self.version_vector.get(event.id().actor);
        if event.id().event_counter > current_id {
            match &self.new_consumer_state {
                &NewConsumerState::Consumer { ref namespace, .. } => namespace.matches(event.namespace()),
                &NewConsumerState::Peer => true,
                _ => false
            }
        } else {
            false
        }
    }

    pub fn update_version_vector(&mut self, id: FloEventId) {
        self.version_vector.update_if_greater(id);
    }

    pub fn consume_from_namespace(&mut self, namespace: NamespaceGlob, limit: u64) -> Result<&VersionVector, String> {
        if self.new_consumer_state.is_not_consuming() {
            self.new_consumer_state = NewConsumerState::Consumer {
                namespace: namespace,
                remaining_events: limit,
            };
            Ok(&self.version_vector)
        } else {
            Err(format!("Cannot start consuming for connection_id: {} because the client state is already: {:?}",
                        self.connection_id,
                        self.new_consumer_state))
        }
    }

    pub fn start_peer_replication(&mut self, version_vec: VersionVector) -> Result<(), String> {
        if self.new_consumer_state.is_not_consuming() {
            self.new_consumer_state = NewConsumerState::Peer;
            Ok(())
        } else {
            Err(format!("Cannot start peer replication for connection_id: {} because state is already: {:?}",
                        self.connection_id,
                        self.new_consumer_state))
        }
    }

    pub fn stop_consuming(&mut self) {
        self.new_consumer_state = NewConsumerState::NotConsuming;
    }

    pub fn send_message(&self, message: ProtocolMessage) -> Result<(), String> {
        self.do_send(ServerMessage::Other(message))
    }

    pub fn send_event(&mut self, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        self.version_vector.update(event.id).and_then(|()| {
            self.do_send(ServerMessage::Event(event))
        })
    }

    fn do_send(&self, message: ServerMessage) -> Result<(), String> {
        self.sender.send(message).map_err(|err| {
            err.description().to_owned()
        })
    }



    ///// Old Client api below; replacing these shit methods with better ones above

    pub fn start_consuming(&mut self, state: ConsumingState) {
        self.consumer_state = ClientState::Consuming(state);
    }

    pub fn event_namespace_matches(&self, event_namespace: &str) -> bool {
        if let ClientState::Consuming(ref state) = self.consumer_state {
            state.namespace.matches(event_namespace)
        } else {
            false
        }
    }

    pub fn send(&mut self, message: ServerMessage) -> Result<(), ClientSendError> {
        let conn_id = self.connection_id;
        trace!("Sending message to client: {} : {:?}", conn_id, message);
        if let &ServerMessage::Event(ref event) = &message {
            self.consumer_state.update_event_sent(*event.id(), self.connection_id);
        }
        self.sender.send(message).map_err(|send_err| {
            ClientSendError(send_err.into_message())
        })
    }

    pub fn is_awaiting_new_event(&self) -> bool {
        match self.consumer_state {
            ClientState::Consuming(ref state) if state.consume_type == ConsumeType::Memory => true,
            _ => false
        }
    }

    pub fn get_current_position(&self) -> FloEventId {
        match self.consumer_state {
            ClientState::NotConsuming(id) => id,
            ClientState::Consuming(ref state) => state.last_event_id
        }
    }

    pub fn set_position(&mut self, new_position: FloEventId) {
        match self.consumer_state {
            ClientState::NotConsuming(ref mut id) => *id = new_position,
            ClientState::Consuming(ref mut state) => {
                debug!("Client: {} moving position from {:?} to {:?} while in consuming state", self.connection_id, state.last_event_id, new_position);
                state.last_event_id = new_position;
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use super::namespace::NamespaceGlob;
    use engine::version_vec::VersionVector;
    use event::{OwnedFloEvent, ActorId, EventCounter, FloEventId};
    use channels::{Sender, MockSender};
    use std::sync::Arc;

    fn globAll() -> NamespaceGlob {
        NamespaceGlob::new("/**/*").unwrap()
    }

    #[test]
    fn start_peer_replication_returns_error_when_client_has_already_started_consuming() {
        let mut subject = subject();
        subject.consume_from_namespace(NamespaceGlob::new("/foo").unwrap(), 999).unwrap();

        let result = subject.start_peer_replication(VersionVector::new());
        assert!(result.is_err());
    }

    #[test]
    fn consume_from_namespace_returns_error_when_client_has_been_previously_upgraded_to_peer() {
        let mut subject = subject();
        subject.start_peer_replication(VersionVector::new()).unwrap();

        let result = subject.consume_from_namespace(NamespaceGlob::new("/foo").unwrap(), 999);
        assert!(result.is_err());
    }

    #[test]
    fn should_send_event_returns_true_when_event_id_is_greater_than_the_one_in_version_vec() {
        let actor = 1;
        let mut subject = subject();
        subject.consume_from_namespace(globAll(), 8888).unwrap();
        subject.update_version_vector(FloEventId::new(actor, 8));

        let event = event(actor, 9, "/what/evar");
        assert!(subject.should_send_event(&*event));
    }

    #[test]
    fn should_send_event_returns_false_when_event_id_is_less_than_or_equal_to_the_id_in_the_version_vec() {
        let actor = 1;
        let mut subject = subject();
        subject.consume_from_namespace(globAll(), 8888).unwrap();
        subject.update_version_vector(FloEventId::new(actor, 8));

        let lessThan = event(actor, 7, "/what/evar");
        assert!(!subject.should_send_event(&*lessThan));

        let equalTo = event(actor, 8, "/what/evar");
        assert!(!subject.should_send_event(&*equalTo));
    }

    #[test]
    fn should_send_event_returns_false_when_namespace_does_not_match() {
        let actor = 1;
        let mut subject = subject();
        let namespaceGlob = NamespaceGlob::new("/this").unwrap();
        subject.consume_from_namespace(namespaceGlob, 8888).unwrap();
        subject.update_version_vector(FloEventId::new(actor, 8));

        let greaterThan = event(actor, 9999, "/what/evar");
        assert!(!subject.should_send_event(&*greaterThan));
    }

    #[test]
    fn should_send_event_returns_false_when_client_has_not_started_consuming() {
        let actor = 1;
        let mut subject = subject();

        let event = event(actor, 9999, "/what/evar");
        assert!(!subject.should_send_event(&*event));
    }

    #[test]
    fn should_send_event_returns_true_when_client_is_a_peer_and_event_is_greater_than_version_vector() {
        let actor = 1;
        let mut subject = subject();

        subject.start_peer_replication(VersionVector::new()).unwrap();
        let event = event(actor, 99, "/any");
        assert!(subject.should_send_event(&*event));
    }

    #[test]
    fn should_send_event_returns_true_if_namespace_matches_for_normal_consumer_and_event_id_is_greater_than_version_vec() {
        let mut subject = subject();

        let namespace = "/food/breakfast/bacon";
        let namespace_glob = NamespaceGlob::new(namespace).unwrap();
        subject.consume_from_namespace(namespace_glob, 8888).unwrap();
        let event = event(4, 2, namespace);

        assert!(subject.should_send_event(&*event));
    }

    #[test]
    fn should_update_version_vector_when_event_is_sent() {
        let mut subject = subject();
        subject.consume_from_namespace(NamespaceGlob::new("/**/*").unwrap(), 999).unwrap();
        let actor = 5;
        let counter = 8;
        let event = event(actor, counter, "/the/ns");
        assert!(subject.should_send_event(&*event));
        subject.send_event(event.clone()).expect("failed to send event");

        assert!(!subject.should_send_event(&*event));

        let result = subject.version_vector.get(actor);
        assert_eq!(counter, result);
    }

    fn event(actor: ActorId, counter: EventCounter, namespace: &str) -> Arc<OwnedFloEvent> {
        Arc::new(OwnedFloEvent::new(FloEventId::new(actor, counter), None, ::time::now(), namespace.to_owned(), Vec::new()))
    }

    fn subject() -> Client<MockSender<ServerMessage>> {
        use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 3333));
        Client::new(1, addr, MockSender::new())
    }

}
