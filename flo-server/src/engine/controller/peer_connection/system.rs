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

}




#[derive(Debug)]
struct OutgoingPeerSystemConnection {
    client_tx: ConnectionControlSender,
}
