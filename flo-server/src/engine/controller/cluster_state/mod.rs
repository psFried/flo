
use std::net::SocketAddr;
use std::time::{Instant};

use event::EventCounter;
use protocol::FloInstanceId;


#[derive(Debug, PartialEq, Clone)]
struct PersistentClusterState {
    current_term: u64,
    voted_for: Option<FloInstanceId>,
}

pub struct ClusterState {
    persistent: PersistentClusterState,
    last_applied: EventCounter,
}

struct Peer {
    id: FloInstanceId,
    leader: bool,
    peer_addr: SocketAddr,
    last_ackgnowledged_entry: EventCounter,
}

enum PeerState {
    Connecting(Instant),
}
