use std::net::SocketAddr;

use engine::event_stream::partition::{PartitionRef, Operation};
use engine::event_stream::EventStreamRef;
use engine::controller::{SystemPartitionSender, SystemOperation, ConnectionRef};
use engine::controller::cluster_state::{SharedClusterState, ClusterStateReader};
use engine::ConnectionId;
use atomics::AtomicBoolReader;


#[derive(Clone, Debug)]
pub struct SystemStreamRef {
    cluster_state_reader: ClusterStateReader,
    system_sender: SystemPartitionSender,
    inner: PartitionRef,
}

impl SystemStreamRef {

    pub fn new(partition_ref: PartitionRef, system_sender: SystemPartitionSender, cluster_state_reader: ClusterStateReader) -> SystemStreamRef {
        SystemStreamRef {
            cluster_state_reader,
            system_sender,
            inner: partition_ref,
        }
    }

    pub fn with_cluster_state<F, T>(&self, fun: F) -> T where F: Fn(&SharedClusterState) -> T {
        let state = self.cluster_state_reader.read().unwrap();
        fun(&*state)
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

    pub fn connection_closed(&mut self, connection_id: ConnectionId) {
        let op = SystemOperation::connection_closed(connection_id);
        self.send(op);
    }

    pub fn outgoing_connection_failed(&mut self, socket_addr: SocketAddr) {
        let op = SystemOperation::outgoing_connection_failed(socket_addr);
        self.send(op);
    }

    pub fn connection_upgraded_to_peer(&mut self, connection_id: ConnectionId) {
        let op = SystemOperation::connection_upgraded_to_peer(connection_id);
        self.send(op);
    }

    fn send(&mut self, op: SystemOperation) {
        self.system_sender.send(op).expect("Failed to send to flo controller. System must have shut down");
    }
}






