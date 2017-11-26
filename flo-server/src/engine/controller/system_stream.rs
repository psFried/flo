use std::net::SocketAddr;

use engine::event_stream::partition::{PartitionRef, Operation};
use engine::event_stream::EventStreamRef;
use engine::ConnectionId;



#[derive(Clone, Debug)]
pub struct SystemStreamRef {
    inner: PartitionRef,
}

impl SystemStreamRef {

    pub fn new(partition_ref: PartitionRef) -> SystemStreamRef {
        SystemStreamRef {
            inner: partition_ref,
        }
    }

    pub fn to_event_stream(&self) -> EventStreamRef {
        let name = self.inner.event_stream_name().to_owned();
        let partition_ref = vec![self.inner.clone()];
        EventStreamRef::new(name, partition_ref)
    }

    pub fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, socket_addr: SocketAddr) {
        let op = Operation::outgoing_connection_failed(connection_id, socket_addr);
        self.inner.send(op).expect("System Stream has shutdown");
    }

}






