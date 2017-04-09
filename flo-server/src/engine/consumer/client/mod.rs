mod connection_manager;
mod context;
mod connection_state;

use event::{FloEvent, FloEventId, ActorId, OwnedFloEvent, VersionVector};
use engine::api::{ConnectionId, ClientConnect, NamespaceGlob, ConsumerState};
use protocol::{ServerMessage, ProtocolMessage};
use channels::Sender;

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

pub type ClientImpl = Client<UnboundedSender<ServerMessage>>;

#[derive(Clone)]
pub struct Client<T: Sender<ServerMessage>> {
    pub connection_id: ConnectionId,
    pub addr: SocketAddr,
    sender: T,
    version_vector: VersionVector,
    new_consumer_state: Option<ConsumerState>,
    batch_size: u64,
}

pub const DEFAULT_BATCH_SIZE: u64 = 10_000;

impl Client<UnboundedSender<ServerMessage>> {
    pub fn from_client_connect(connect_message: ClientConnect) -> Client<UnboundedSender<ServerMessage>> {
        Client {
            connection_id: connect_message.connection_id,
            addr: connect_message.client_addr,
            sender: connect_message.message_sender,
            version_vector: VersionVector::new(),
            new_consumer_state: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl <T: Sender<ServerMessage>> Client<T> {

    #[cfg(test)]
    fn new(connection_id: ConnectionId, addr: SocketAddr, sender: T) -> Client<T> {
        Client {
            connection_id: connection_id,
            addr: addr,
            sender: sender,
            version_vector: VersionVector::new(),
            new_consumer_state: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn get_batch_size(&self) -> u64 {
        self.batch_size
    }

    pub fn should_send_event(&self, event: &OwnedFloEvent) -> bool {
        self.new_consumer_state.as_ref().map(|state| {
            state.should_send_event(event)
        }).unwrap_or(false)
    }

    pub fn update_version_vector(&mut self, id: FloEventId) {
        self.version_vector.update_if_greater(id);
    }

    pub fn get_version_vec(&self) -> &VersionVector {
        &self.version_vector
    }

    pub fn consume_from_namespace(&mut self, namespace: NamespaceGlob, limit: u64) -> Result<&VersionVector, String> {
        unimplemented!()
    }

    pub fn start_peer_replication(&mut self, from_actor: ActorId, version_vec: VersionVector) -> Result<(), String> {
        unimplemented!()
    }

    pub fn stop_consuming(&mut self) {
        self.new_consumer_state = None;
    }

    pub fn send_message(&self, message: ProtocolMessage) -> Result<(), String> {
        self.do_send(ServerMessage::Other(message))
    }

    pub fn send_message_log_error(&self, message: ProtocolMessage, log_if_failure: &'static str) {
        if let Err(err) = self.do_send(ServerMessage::Other(message)) {
            warn!("Failed to send message to connection_id: {} - {} caused by: {}", self.connection_id, log_if_failure, err);
        }
    }

    pub fn send_event(&mut self, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        unimplemented!()
    }

    fn do_send(&self, message: ServerMessage) -> Result<(), String> {
        self.sender.send(message).map_err(|err| {
            err.description().to_owned()
        })
    }

}


#[cfg(test)]
mod test {
    use super::*;
    use engine::api::NamespaceGlob;
    use event::{OwnedFloEvent, ActorId, EventCounter, FloEventId, VersionVector};
    use channels::MockSender;
    use std::sync::Arc;

    fn glob_all() -> NamespaceGlob {
        NamespaceGlob::new("/**/*").unwrap()
    }

    #[test]
    fn consumer_returns_to_not_consuming_state_when_event_limit_is_reached() {
        let limit = 3;
        let mut subject = subject();

        subject.consume_from_namespace(NamespaceGlob::new("/**/*").unwrap(), limit).unwrap();

        for i in 0..(limit) {
            let event = event(5, i + 1, "/internet/porn");
            subject.send_event(event).unwrap();
        }
        assert!(subject.new_consumer_state.is_none());
    }


    #[test]
    fn start_peer_replication_returns_error_when_client_has_already_started_consuming() {
        let mut subject = subject();
        subject.consume_from_namespace(NamespaceGlob::new("/foo").unwrap(), 999).unwrap();

        let result = subject.start_peer_replication(1, VersionVector::new());
        assert!(result.is_err());
    }

    #[test]
    fn consume_from_namespace_returns_error_when_client_has_been_previously_upgraded_to_peer() {
        let mut subject = subject();
        subject.start_peer_replication(1, VersionVector::new()).unwrap();

        let result = subject.consume_from_namespace(NamespaceGlob::new("/foo").unwrap(), 999);
        assert!(result.is_err());
    }

    #[test]
    fn should_send_event_returns_true_when_event_id_is_greater_than_the_one_in_version_vec() {
        let actor = 1;
        let mut subject = subject();
        subject.consume_from_namespace(glob_all(), 8888).unwrap();
        subject.update_version_vector(FloEventId::new(actor, 8));

        let event = event(actor, 9, "/what/evar");
        assert!(subject.should_send_event(&*event));
    }

    #[test]
    fn should_send_event_returns_false_when_event_id_is_less_than_or_equal_to_the_id_in_the_version_vec() {
        let actor = 1;
        let mut subject = subject();
        subject.consume_from_namespace(glob_all(), 8888).unwrap();
        subject.update_version_vector(FloEventId::new(actor, 8));

        let less_than = event(actor, 7, "/what/evar");
        assert!(!subject.should_send_event(&*less_than));

        let equal_to = event(actor, 8, "/what/evar");
        assert!(!subject.should_send_event(&*equal_to));
    }

    #[test]
    fn should_send_event_returns_false_when_namespace_does_not_match() {
        let actor = 1;
        let mut subject = subject();
        let ns_glob = NamespaceGlob::new("/this").unwrap();
        subject.consume_from_namespace(ns_glob, 8888).unwrap();
        subject.update_version_vector(FloEventId::new(actor, 8));

        let wrong_namespace = event(actor, 9999, "/what/evar");
        assert!(!subject.should_send_event(&*wrong_namespace));
    }

    #[test]
    fn should_send_event_returns_false_when_client_has_not_started_consuming() {
        let actor = 1;
        let subject = subject();

        let event = event(actor, 9999, "/what/evar");
        assert!(!subject.should_send_event(&*event));
    }

    #[test]
    fn should_send_event_returns_true_when_client_is_a_peer_and_event_is_greater_than_version_vector() {
        let actor = 1;
        let event_actor = 2;
        let mut subject = subject();

        subject.start_peer_replication(actor, VersionVector::new()).unwrap();
        let event = event(event_actor, 99, "/any");
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
    fn send_event_updates_the_version_vector() {
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

    #[test]
    fn send_event_returns_error_when_client_is_in_initial_state() {
        let mut subject = subject();
        let result = subject.send_event(event(3, 4, "/ns"));
        assert!(result.is_err());
    }

    #[test]
    fn send_event_returns_err_when_event_id_is_less_than_or_equal_to_the_one_in_the_version_vector_for_that_actor() {
        let mut subject = subject();
        subject.consume_from_namespace(NamespaceGlob::new("/**/*").unwrap(), 999).unwrap();
        let actor = 5;
        let counter = 8;
        let event = event(actor, counter, "/the/ns");
        subject.send_event(event.clone()).expect("failed to send event");

        //send the same event twice
        let result = subject.send_event(event);
        assert!(result.is_err());
    }

    fn event(actor: ActorId, counter: EventCounter, namespace: &str) -> Arc<OwnedFloEvent> {
        Arc::new(OwnedFloEvent::new(FloEventId::new(actor, counter), None, ::event::time::now(), namespace.to_owned(), Vec::new()))
    }

    fn subject() -> Client<MockSender<ServerMessage>> {
        use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 3333));
        Client::new(1, addr, MockSender::new())
    }

}
