use std::net::SocketAddr;
use std::time::Instant;

use futures::sync::mpsc::UnboundedSender;

use engine::event_stream::partition::{self, Operation};
use engine::connection_handler::{ConnectionControl, ConnectionControlSender};
use engine::ConnectionId;

#[derive(Debug)]
pub struct ConnectionRef {
    pub connection_id: ConnectionId,
    pub remote_address: SocketAddr,
    pub control_sender: ConnectionControlSender,
}

#[derive(Debug)]
pub enum SystemOpType {
    Tick,
    PartitionOp(partition::OpType),
    IncomingConnectionEstablished(ConnectionRef),
    ConnectionUpgradeToPeer,
    ConnectionClosed,
    OutgoingConnectionFailed(SocketAddr),
}

#[derive(Debug)]
pub struct SystemOperation {
    pub connection_id: ConnectionId,
    pub op_start_time: Instant,
    pub op_type: SystemOpType,
}

impl SystemOperation {

    pub fn connection_upgraded_to_peer(connection_id: ConnectionId) -> SystemOperation {
        SystemOperation::new(connection_id, SystemOpType::ConnectionUpgradeToPeer)
    }

    pub fn incoming_connection_established(connection: ConnectionRef) -> SystemOperation {
        SystemOperation::new(connection.connection_id, SystemOpType::IncomingConnectionEstablished(connection))
    }

    pub fn connection_closed(connection_id: ConnectionId) -> SystemOperation {
        SystemOperation::new(connection_id, SystemOpType::ConnectionClosed)
    }

    pub fn outgoing_connection_failed(addr: SocketAddr) -> SystemOperation {
        SystemOperation::new(0, SystemOpType::OutgoingConnectionFailed(addr))
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
