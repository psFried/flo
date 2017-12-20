use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;

use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
#[allow(deprecated)]
use tokio_core::io::Io;
use futures::{Future, Stream, Sink};

use protocol::{FloInstanceId, Term};
use event::EventCounter;
use event_loops::LoopHandles;
use engine::ConnectionId;
use engine::controller::SystemStreamRef;
use engine::connection_handler::ConnectionControlSender;

use super::ConnectionSendResult;

pub struct CallAppendEntries {
    pub leader_id: FloInstanceId,
    pub term: Term,
    pub prev_entry_term: Term,
    pub prev_entry_index: EventCounter,
    pub leader_commit_index: EventCounter,
}

/// Trait representing an active peer connection, with functions to control it
pub trait PeerSystemConnection: Debug + Send + 'static {
    fn connection_id(&self) -> ConnectionId;
}


#[derive(Debug)]
pub struct PeerConnectionImpl {
    connection_id: ConnectionId,
    sender: ConnectionControlSender
}

impl PeerConnectionImpl {
    pub fn new(connection_id: ConnectionId, sender: ConnectionControlSender) -> PeerConnectionImpl {
        PeerConnectionImpl { connection_id, sender }
    }
}

impl PeerSystemConnection for PeerConnectionImpl {
    fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }
}

