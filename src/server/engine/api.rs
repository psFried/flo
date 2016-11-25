
use flo_event::{FloEventId, FloEvent, OwnedFloEvent};

use event::Event;
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use std::sync::atomic;
use std::sync::Arc;
use std::fmt::{self, Debug};

pub type ConnectionId = usize;

static CURRENT_CLIENT_ID: atomic::AtomicUsize = atomic::ATOMIC_USIZE_INIT;

fn next_connection_id() -> ConnectionId {
    CURRENT_CLIENT_ID.fetch_add(1, atomic::Ordering::SeqCst)
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

#[derive(Debug, PartialEq)]
pub struct ProduceEvent {
    connection_id: ConnectionId,
    op_id: u64,
    event_data: Vec<u8>
}
unsafe impl Send for ProduceEvent {}

#[derive(Debug)]
pub enum ClientMessage {
    ClientConnect(NewClient),
    Produce(ProduceEvent),
    UpdateMarker(FloEventId),
    StartConsuming
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

