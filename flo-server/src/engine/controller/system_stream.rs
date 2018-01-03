use std::net::SocketAddr;

use protocol::FloInstanceId;
use engine::event_stream::partition::{PartitionRef, Operation, SharedReaderRefs, PartitionReader, SegmentNum};
use engine::event_stream::EventStreamRef;
use engine::controller::{SystemPartitionSender, SystemOperation, ConnectionRef, Peer, CallRequestVote, VoteResponse, SystemStreamReader, ReceiveAppendEntries};
use engine::controller::cluster_state::{SharedClusterState, ClusterStateReader};
use engine::ConnectionId;
use atomics::{AtomicBoolReader, AtomicCounterReader};


#[derive(Clone, Debug)]
pub struct SystemStreamRef {
    cluster_state_reader: ClusterStateReader,
    system_sender: SystemPartitionSender,
    inner: PartitionRef,
    system_segment_readers: SharedReaderRefs,
}

impl SystemStreamRef {

    pub fn new(partition_ref: PartitionRef, system_sender: SystemPartitionSender, cluster_state_reader: ClusterStateReader, readers: SharedReaderRefs) -> SystemStreamRef {
        SystemStreamRef {
            cluster_state_reader,
            system_sender,
            inner: partition_ref,
            system_segment_readers: readers
        }
    }

    pub fn create_system_stream_reader(&self, connection_id: ConnectionId) -> SystemStreamReader {
        debug!("Creating new SystemStreamReader for connection_id: {}", connection_id);
        SystemStreamReader::new(connection_id, self.system_segment_readers.clone(), self.inner.get_highest_counter_reader())
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

    pub fn tick(&mut self) -> Result<(), ()> {
        let op = SystemOperation::tick();
        self.system_sender.send(op).map_err(|_| ())
    }

    pub fn tick_error(&mut self) {
        // TODO: send a message to the system partition to let it know that there was an error so that it can resign as primary
        unimplemented!()
    }

    pub fn incoming_connection_accepted(&mut self, connection_ref: ConnectionRef) {
        let op = SystemOperation::incoming_connection_established(connection_ref);
        self.send(op);
    }

    pub fn connection_closed(&mut self, connection_id: ConnectionId) {
        let op = SystemOperation::connection_closed(connection_id);
        self.send(op);
    }

    pub fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, socket_addr: SocketAddr) {
        let op = SystemOperation::outgoing_connection_failed(connection_id, socket_addr);
        self.send(op);
    }

    pub fn connection_upgraded_to_peer(&mut self, connection_id: ConnectionId, peer_id: FloInstanceId, system_primary: Option<Peer>, cluster_members: Vec<Peer>) {
        let op = SystemOperation::connection_upgraded_to_peer(connection_id, peer_id, system_primary, cluster_members);
        self.send(op);
    }

    pub fn request_vote_received(&mut self, connection_id: ConnectionId, request: CallRequestVote) {
        let op = SystemOperation::request_vote_received(connection_id, request);
        self.send(op);
    }

    pub fn vote_response_received(&mut self, connection_id: ConnectionId, response: VoteResponse) {
        let op = SystemOperation::vote_response_received(connection_id, response);
        self.send(op);
    }

    pub fn append_entries_received(&mut self, connection_id: ConnectionId, append: ReceiveAppendEntries) {
        let op = SystemOperation::append_entries_received(connection_id, append);
        self.send(op);
    }

    fn send(&mut self, op: SystemOperation) {
        // TODO: change this to propagate the error so that connectionHandlers can shut down gracefully
        self.system_sender.send(op).expect("Failed to send to flo controller. System must have shut down");
    }
}







