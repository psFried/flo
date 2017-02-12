use protocol::ServerMessage;
use flo_event::{FloEventId, ActorId, EventCounter, OwnedFloEvent};
use engine::version_vec::VersionVector;

use futures::sync::mpsc::UnboundedSender;

use std::time::Instant;
use std::sync::atomic;
use std::fmt::{self, Debug};
use std::collections::HashMap;
use std::net::SocketAddr;

pub type ConnectionId = usize;

static CURRENT_CONNECTION_ID: atomic::AtomicUsize = atomic::ATOMIC_USIZE_INIT;

pub fn next_connection_id() -> ConnectionId {
    CURRENT_CONNECTION_ID.fetch_add(1, atomic::Ordering::SeqCst)
}

#[derive(Debug, PartialEq, Clone)]
pub struct PeerVersionMap {
    pub connection_id: ConnectionId,
    pub from_actor: ActorId,
    pub actor_versions: VersionVector,
}
unsafe impl Send for PeerVersionMap {}

#[derive(Debug, PartialEq, Clone)]
pub struct StateDeltaHeader {
    pub connection_id: ConnectionId,
    pub from_actor: ActorId,
    pub actor_versions: VersionVector,
    pub event_count: u32,
}
unsafe impl Send for StateDeltaHeader {}

#[derive(Debug, PartialEq, Clone)]
pub struct PeerUpdate {
    pub connection_id: ConnectionId,
    pub actor_id: ActorId,
    pub version_map: VersionVector,
}
unsafe impl Send for PeerUpdate {}

#[derive(Debug, PartialEq, Clone)]
pub struct PeerAnnounce {
    pub connection_id: ConnectionId,
    pub actor_id: ActorId,
}
unsafe impl Send for PeerAnnounce {}

#[derive(Debug, PartialEq, Clone)]
pub enum ConsumerMessage {
    ClientConnect(ClientConnect),
    ClientAuth(ClientAuth),
    UpdateMarker(ConnectionId, FloEventId),
    StartConsuming(ConnectionId, String, i64),
    ContinueConsuming(ConnectionId, FloEventId, i64),
    Disconnect(ConnectionId),
    EventPersisted(ConnectionId, OwnedFloEvent),
    EventLoaded(ConnectionId, OwnedFloEvent),
    StartPeerReplication(PeerVersionMap),
}
unsafe impl Send for ConsumerMessage {}

#[derive(Debug, PartialEq, Clone)]
pub enum ProducerMessage {
    ClientConnect(ClientConnect),
    ClientAuth(ClientAuth),
    Produce(ProduceEvent),
    Disconnect(ConnectionId),
    PeerAnnounce(PeerVersionMap),
    ReplicateEvent(ConnectionId, OwnedFloEvent, Instant),
    PeerConnectFailed(SocketAddr),
}
unsafe impl Send for ProducerMessage {}

#[derive(Debug, PartialEq, Clone)]
pub enum ClientMessage {
    Consumer(ConsumerMessage),
    Producer(ProducerMessage),
    Both(ConsumerMessage, ProducerMessage),
}
unsafe impl Send for ClientMessage {}

#[derive(Clone)]
pub struct ClientConnect {
    pub connection_id: ConnectionId,
    pub client_addr: SocketAddr,
    pub message_sender: UnboundedSender<ServerMessage>,
}
unsafe impl Send for ClientConnect {}

impl Debug for ClientConnect {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(formatter, "NewClient {{ connection_id: {}, client_addr: {:?} }}", self.connection_id, self.client_addr)
    }
}

impl PartialEq for ClientConnect {
    fn eq(&self, other: &ClientConnect) -> bool {
        self.connection_id == other.connection_id &&
                self.client_addr == other.client_addr &&
                &(self.message_sender) as * const UnboundedSender<ServerMessage> == &(other.message_sender) as * const UnboundedSender<ServerMessage>
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ClientAuth {
    pub connection_id: ConnectionId,
    pub namespace: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ProduceEvent {
    pub message_recv_start: Instant,
    pub namespace: String,
    pub connection_id: ConnectionId,
    pub op_id: u32,
    pub parent_id: Option<FloEventId>,
    pub event_data: Vec<u8>
}
unsafe impl Send for ProduceEvent {}

