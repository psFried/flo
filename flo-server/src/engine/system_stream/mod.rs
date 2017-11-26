mod flo_instance_id;
mod replication_plan;

use std::net::SocketAddr;

use event::EventCounter;
use engine::ConnectionId;
use engine::event_stream::EventStreamOptions;

pub use self::flo_instance_id::FloInstanceId;
pub use self::replication_plan::{SystemReplicationPlan, StreamReplicationPlan, PartitionReplicationPlan};

pub type Term = u64;


#[derive(Debug, PartialEq, Clone)]
pub struct FloServer {
    pub id: FloInstanceId,
    pub address: SocketAddr,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ServerStatus {
    peer_id: FloInstanceId,
    leader_id: FloInstanceId,
    system_event_counter: EventCounter,
    current_term: Term,
    all_members: Vec<FloServer>,
}

#[derive(Debug, Clone)]
pub enum SystemOp {
    OutgoingConnectionFailed(SocketAddr),
    PeerConnectionEstablished(ConnectionId, SocketAddr),
    AppendResponse(AppendEntriesResponse),
    VoteResponse(RequestVoteResponse),
}



#[derive(Debug, PartialEq, Clone)]
pub struct AppendEntriesCall {
    pub leader_id: FloInstanceId,
    pub term: Term,
    pub prev_entry_term: Term,
    pub prev_entry_index: EventCounter,
    pub leader_commit_index: EventCounter,
    pub entries: Vec<SystemLogEntry>,
}

impl AppendEntriesCall {
    pub fn heartbeat(leader_id: FloInstanceId,
                     term: Term,
                     prev_entry_term: Term,
                     prev_entry_index: EventCounter,
                     leader_commit_index: EventCounter) -> AppendEntriesCall {

        AppendEntriesCall {
            leader_id,
            term,
            prev_entry_term,
            prev_entry_index,
            leader_commit_index,
            entries: Vec::new(),
        }
    }

    pub fn is_heartbeat(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct AppendEntriesResponse {
    pub from_peer: FloInstanceId,
    pub term: Term,
    pub success: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RequestVoteCall {
    pub term: Term,
    pub candidate_id: FloInstanceId,
    pub last_log_index: EventCounter,
    pub last_log_term: Term,
}


#[derive(Debug, PartialEq, Clone)]
pub struct RequestVoteResponse {
    pub from_peer: FloInstanceId,
    pub term: Term,
    pub vote_granted: bool,
}


#[derive(Debug, Clone, PartialEq)]
pub enum SystemLogEntry {
    /// Communicates which server should be the primary for each partition in each event stream
    SetReplicationPlan(SystemReplicationPlan),

    /// Command to create a new event stream. Each follower should create the stream, but wait for
    /// a replication plan before allowing any writes to it
    CreateEventStream(EventStreamOptions),

    /// First phase of adding a new member to the cluster. New members are NOT eligible to become
    /// primary until an `ActivateClusterMember` command with its id has been committed to the log.
    AddClusterMember(FloServer),

    /// Once this entry has been committed, the given server is now eligible to be elected primary
    ActivateClusterMember(FloInstanceId),
}
