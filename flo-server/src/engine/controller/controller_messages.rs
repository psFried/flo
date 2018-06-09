use std::net::SocketAddr;
use std::time::Instant;

use futures::sync::mpsc::UnboundedSender;

use event::{EventCounter, OwnedFloEvent};
use protocol::{FloInstanceId, Term};
use engine::event_stream::partition::{self, Operation};
use engine::connection_handler::{ConnectionControl, ConnectionControlSender};
use engine::ConnectionId;

#[derive(Debug, Clone, PartialEq)]
pub struct CallRequestVote {
    pub term: Term,
    pub candidate_id: FloInstanceId,
    pub last_log_index: EventCounter,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VoteResponse {
    pub term: Term,
    pub granted: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectionRef {
    pub connection_id: ConnectionId,
    pub remote_address: SocketAddr,
    pub control_sender: ConnectionControlSender,
}

impl PartialEq for ConnectionRef {
    fn eq(&self, other: &ConnectionRef) -> bool {
        self.connection_id == other.connection_id && self.remote_address == other.remote_address
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveAppendEntries {
    pub term: Term,
    pub prev_entry_index: EventCounter,
    pub prev_entry_term: Term,
    pub commit_index: EventCounter,
    pub events: Vec<OwnedFloEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: Option<EventCounter>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub id: FloInstanceId,
    pub address: SocketAddr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PeerUpgrade {
    pub peer: Peer,
    pub system_primary: Option<Peer>,
    pub cluster_members: Vec<Peer>,
}


#[derive(Debug, PartialEq)]
pub enum SystemOpType {
    Tick,
    PartitionOp(partition::OpType),
    IncomingConnectionEstablished(ConnectionRef),
    ConnectionUpgradeToPeer(PeerUpgrade),
    ConnectionClosed,
    OutgoingConnectionFailed(SocketAddr),

    RequestVote(CallRequestVote),
    VoteResponseReceived(VoteResponse),
    AppendEntriesReceived(ReceiveAppendEntries),
    AppendEntriesResponseReceived(AppendEntriesResponse),
}

impl SystemOpType {
    pub fn is_tick(&self) -> bool {
        match self {
            &SystemOpType::Tick => true,
            _ => false
        }
    }
}

#[derive(Debug)]
pub struct SystemOperation {
    pub connection_id: ConnectionId,
    pub op_start_time: Instant,
    pub op_type: SystemOpType,
}

impl SystemOperation {

    pub fn append_entries_response_received(connection_id: ConnectionId, response: AppendEntriesResponse) -> SystemOperation {
        SystemOperation::new(connection_id, SystemOpType::AppendEntriesResponseReceived(response))
    }

    pub fn append_entries_received(connection_id: ConnectionId, append: ReceiveAppendEntries) -> SystemOperation {
        SystemOperation::new(connection_id, SystemOpType::AppendEntriesReceived(append))
    }

    pub fn vote_response_received(connection_id: ConnectionId, response: VoteResponse) -> SystemOperation {
        SystemOperation::new(connection_id, SystemOpType::VoteResponseReceived(response))
    }

    pub fn request_vote_received(connection_id: ConnectionId, request: CallRequestVote) -> SystemOperation {
        SystemOperation::new(connection_id, SystemOpType::RequestVote(request))
    }

    pub fn connection_upgraded_to_peer(connection_id: ConnectionId, peer: Peer, system_primary: Option<Peer>, cluster_members: Vec<Peer>) -> SystemOperation {
        let upgrade = PeerUpgrade {
            peer,
            system_primary,
            cluster_members
        };
        SystemOperation::new(connection_id, SystemOpType::ConnectionUpgradeToPeer(upgrade))
    }

    pub fn incoming_connection_established(connection: ConnectionRef) -> SystemOperation {
        SystemOperation::new(connection.connection_id, SystemOpType::IncomingConnectionEstablished(connection))
    }

    pub fn connection_closed(connection_id: ConnectionId) -> SystemOperation {
        SystemOperation::new(connection_id, SystemOpType::ConnectionClosed)
    }

    pub fn outgoing_connection_failed(connection_id: ConnectionId, addr: SocketAddr) -> SystemOperation {
        SystemOperation::new(connection_id, SystemOpType::OutgoingConnectionFailed(addr))
    }

    pub fn tick() -> SystemOperation {
        SystemOperation::new(0, SystemOpType::Tick)
    }

    fn new(connection_id: ConnectionId, op_type: SystemOpType) -> SystemOperation {
        SystemOperation {
            connection_id,
            op_type,
            op_start_time: Instant::now(),
        }
    }
}


impl From<Operation> for SystemOperation {
    fn from(op: Operation) -> SystemOperation {
        let Operation { connection_id, client_message_recv_time, op_type } = op;
        SystemOperation {
            connection_id,
            op_start_time: client_message_recv_time,
            op_type: SystemOpType::PartitionOp(op_type),
        }
    }
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use engine::connection_handler::{create_connection_control_channels, ConnectionControlReceiver};

    pub fn mock_connection_ref(connection_id: ConnectionId, addr: SocketAddr) -> (ConnectionRef, ConnectionControlReceiver) {
        let (tx, rx) = create_connection_control_channels();
        let conn = ConnectionRef {
            connection_id,
            remote_address: addr,
            control_sender: tx,
        };
        (conn, rx)
    }
}
