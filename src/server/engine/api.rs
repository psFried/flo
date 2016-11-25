
use flo_event::{FloEventId, FloEvent, OwnedFloEvent};

use event::Event;
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use std::sync::atomic;
use std::sync::Arc;
use std::fmt::{self, Debug};

pub type ConnectionId = usize;

static CURRENT_CONNECTION_ID: atomic::AtomicUsize = atomic::ATOMIC_USIZE_INIT;

pub fn next_connection_id() -> ConnectionId {
    CURRENT_CONNECTION_ID.fetch_add(1, atomic::Ordering::SeqCst)
}

pub struct NewClient {
    connection_id: ConnectionId,
    message_sender: UnboundedSender<ServerMessage>,
    namespace: String,
    username: String,
    password: String,
}
unsafe impl Send for NewClient {}

impl Debug for NewClient {
    //TODO: remove password from formatted output once things kinda work
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(formatter, "NewClient {{ connection_id: {}, namespace: '{}', username: '{}', password: '{}' }}",
                self.connection_id,
                self.namespace,
                self.username,
                self.password)
    }
}

impl PartialEq for NewClient {
    fn eq(&self, other: &NewClient) -> bool {
        self.connection_id == other.connection_id &&
                self.namespace == other.namespace &&
                self.username == other.username &&
                self.password == other.password &&
                &(self.message_sender) as * const UnboundedSender<ServerMessage> == &(other.message_sender) as * const UnboundedSender<ServerMessage>
    }
}

#[derive(Debug, PartialEq)]
pub struct ProduceEvent {
    connection_id: ConnectionId,
    op_id: u64,
    event_data: Vec<u8>
}
unsafe impl Send for ProduceEvent {}

#[derive(Debug, PartialEq)]
pub enum ClientMessage {
    ClientConnect(NewClient),
    Produce(ProduceEvent),
    UpdateMarker(FloEventId),
    StartConsuming,
    Disconnect,
}
unsafe impl Send for ClientMessage {}

#[derive(Debug, PartialEq)]
pub struct EventAck {
    connection_id: ConnectionId,
    op_id: u64,
    event_id: FloEventId,
}
unsafe impl Send for EventAck {}

#[derive(Debug, PartialEq)]
pub enum ServerMessage {
    EventPersisted(EventAck),
    Event(Arc<OwnedFloEvent>),
}
unsafe impl Send for ServerMessage {}

