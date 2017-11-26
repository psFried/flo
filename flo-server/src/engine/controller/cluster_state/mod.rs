
use std::net::SocketAddr;
use std::time::{Instant, Duration};

use event::EventCounter;
use engine::controller::outgoing_io::OutgoingConnectionCreator;
use engine::system_stream::FloInstanceId;


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
