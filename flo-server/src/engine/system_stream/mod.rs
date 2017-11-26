mod flo_instance_id;

use std::net::SocketAddr;

use event::EventCounter;
use engine::ConnectionId;
pub use self::flo_instance_id::FloInstanceId;


#[derive(Debug, PartialEq)]
pub struct FloServer {
    pub id: FloInstanceId,
    pub address: SocketAddr,
}

#[derive(Debug, PartialEq)]
pub struct ServerStatus {
    peer_id: FloInstanceId,
    leader_id: FloInstanceId,
    system_event_counter: EventCounter,
    current_term: u64,
    all_members: Vec<FloServer>,
}

#[derive(Debug)]
pub enum SystemOp {
    OutgoingConnectionFailed(SocketAddr),
    PeerConnectionEstablished(ConnectionId, SocketAddr),
}


