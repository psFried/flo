use std::net::SocketAddr;

use engine::event_stream::partition::{PartitionRef, Operation};
use engine::event_stream::EventStreamRef;
use engine::controller::{SystemPartitionSender, SystemOperation, ConnectionRef};
use engine::ConnectionId;



#[derive(Clone, Debug)]
pub struct SystemStreamRef {
    system_sender: SystemPartitionSender,
    inner: PartitionRef,
}

impl SystemStreamRef {

    pub fn new(partition_ref: PartitionRef, system_sender: SystemPartitionSender) -> SystemStreamRef {
        SystemStreamRef {
            system_sender,
            inner: partition_ref,
        }
    }

    pub fn to_event_stream(&self) -> EventStreamRef {
        let name = self.inner.event_stream_name().to_owned();
        let partition_ref = vec![self.inner.clone()];
        EventStreamRef::new(name, partition_ref)
    }

    pub fn incomming_connection_accepted(&mut self, connection_ref: ConnectionRef) {
        let op = SystemOperation::incoming_connection_established(connection_ref);
        self.send(op);
    }

    pub fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, socket_addr: SocketAddr) {
        unimplemented!()
    }

    fn send(&mut self, op: SystemOperation) {
        self.system_sender.send(op).expect("Failed to send to flo controller. System must have shut down");
    }
}






