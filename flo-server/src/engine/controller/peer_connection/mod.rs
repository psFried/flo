mod replication;
mod system;

use std::fmt::Debug;
use std::net::SocketAddr;
use std::io;

use engine::EngineRef;
use event_loops::LoopHandles;

pub use self::replication::PeerReplicationConnection;
pub use self::system::PeerSystemConnection;

pub type ConnectionSendResult<T> = Result<(), T>;


/// Trait for creating outgoing connections (clever name, I know).
pub trait OutgoingConnectionCreator {
    fn establish_system_connection(&mut self, address: SocketAddr) -> Box<PeerSystemConnection>;
    fn establish_replication_connection(&mut self, address: SocketAddr, event_stream: String) -> Box<PeerReplicationConnection>;
}

pub struct OutgoingConnectionCreatorImpl {
    event_loops: LoopHandles,
    engine_ref: EngineRef,
}

impl OutgoingConnectionCreator for OutgoingConnectionCreatorImpl {
    fn establish_system_connection(&mut self, address: SocketAddr) -> Box<PeerSystemConnection> {
        unimplemented!()
    }

    fn establish_replication_connection(&mut self, address: SocketAddr, event_stream: String) -> Box<PeerReplicationConnection> {
        unimplemented!()
    }
}





