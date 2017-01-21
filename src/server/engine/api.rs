use protocol::ServerMessage;
use flo_event::{FloEventId, OwnedFloEvent};

use futures::sync::mpsc::UnboundedSender;
use std::sync::atomic;
use std::sync::Arc;
use std::fmt::{self, Debug};

pub type ConnectionId = usize;

static CURRENT_CONNECTION_ID: atomic::AtomicUsize = atomic::ATOMIC_USIZE_INIT;

pub fn next_connection_id() -> ConnectionId {
    CURRENT_CONNECTION_ID.fetch_add(1, atomic::Ordering::SeqCst)
}

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
}
unsafe impl Send for ConsumerMessage {}

#[derive(Debug, PartialEq, Clone)]
pub enum ProducerMessage {
    ClientConnect(ClientConnect),
    ClientAuth(ClientAuth),
    Produce(ProduceEvent),
    Disconnect(ConnectionId),
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
    pub client_addr: ::std::net::SocketAddr,
    pub message_sender: UnboundedSender<ServerMessage<Arc<OwnedFloEvent>>>,
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
                &(self.message_sender) as * const UnboundedSender<ServerMessage<Arc<OwnedFloEvent>>> == &(other.message_sender) as * const UnboundedSender<ServerMessage<Arc<OwnedFloEvent>>>
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
    pub namespace: String,
    pub connection_id: ConnectionId,
    pub op_id: u32,
    pub parent_id: Option<FloEventId>,
    pub event_data: Vec<u8>
}
unsafe impl Send for ProduceEvent {}

