mod persistent;

use std::io;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use std::time::{Instant};
use std::path::Path;

use event::EventCounter;
use engine::{ConnectionId, EngineRef};
use protocol::FloInstanceId;
use atomics::{AtomicBoolWriter, AtomicBoolReader};
use super::ClusterOptions;
use super::peer_connection::{PeerSystemConnection, OutgoingConnectionCreator, OutgoingConnectionCreatorImpl};

pub use self::persistent::{FilePersistedState, PersistentClusterState};

pub trait ConsensusProcessor: Send {
    fn peer_connection_established(&mut self, peer_id: FloInstanceId, connection_id: ConnectionId);
    fn outgoing_connection_failed(&mut self, address: SocketAddr);
}

#[derive(Debug)]
pub struct ClusterManager {
    primary_status_writer: AtomicBoolWriter,
    starting_peer_addresses: Vec<SocketAddr>,
    last_applied: EventCounter,
    persistent: FilePersistedState,
    system_partion_primary_address: SystemPrimaryAddressRef,
    shared: Arc<RwLock<SharedClusterState>>,
    outgoing_connection_creator: Box<OutgoingConnectionCreator>,
}

impl ConsensusProcessor for ClusterManager {

    fn outgoing_connection_failed(&mut self, address: SocketAddr) {
        unimplemented!()
    }
    fn peer_connection_established(&mut self, peer_id: FloInstanceId, connection_id: ConnectionId) {
        unimplemented!()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SharedClusterState {
    pub this_instance_id: FloInstanceId,
    pub this_address: Option<SocketAddr>,
    pub system_primary: Option<Peer>,
    pub peers: Vec<Peer>,
}

impl SharedClusterState {
    pub fn non_cluster() -> SharedClusterState {
        SharedClusterState {
            this_instance_id: FloInstanceId::generate_new(),
            this_address: None,
            system_primary: None,
            peers: Vec::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Peer {
    pub id: FloInstanceId,
    pub address: SocketAddr,
}


pub type SystemPrimaryAddressRef = Arc<RwLock<Option<SocketAddr>>>;
pub type ClusterStateReader = Arc<RwLock<SharedClusterState>>;


//pub fn init_consensus_processor(storage_dir: &Path,
//                                options: Option<ClusterOptions>,
//                                engine_ref: EngineRef,
//                                mut system_primary: AtomicBoolWriter,
//                                primary_address: SystemPrimaryAddressRef) -> io::Result<(Box<ConsensusProcessor>, ClusterStateReader)> {
//
//    if let Some(options) = options {
//        system_primary.set(false);
//        debug!("Initializing in clustered mode, starting as secondary");
//        init_cluster_consensus_processor(storage_dir, options, engine_ref, system_primary, primary_address)
//    } else {
//        debug!("Initializing in non-cluster mode, system stream will always be considered primary");
//        system_primary.set(true);
//        Ok(init_no_op_consensus_processor())
//    }
//}

pub fn init_cluster_consensus_processor(persistent_state: FilePersistedState,
                                    options: ClusterOptions,
                                    engine_ref: EngineRef,
                                    shared_state_ref: ClusterStateReader,
                                    system_primary: AtomicBoolWriter,
                                    primary_address: SystemPrimaryAddressRef) -> Box<ConsensusProcessor> {

    let ClusterOptions{ peer_addresses, event_loop_handles, .. } = options;

    let outgoing_connection_creator = OutgoingConnectionCreatorImpl::new(event_loop_handles, engine_ref);

    let state = ClusterManager {
        primary_status_writer: system_primary,
        last_applied: 0,
        starting_peer_addresses: peer_addresses,
        persistent: persistent_state,
        system_partion_primary_address: primary_address,
        shared: shared_state_ref,
        outgoing_connection_creator: Box::new(outgoing_connection_creator)
    };
    Box::new(state)
}

fn init_no_op_consensus_processor() -> (Box<ConsensusProcessor>, ClusterStateReader) {
    (Box::new(NoOpConsensusProcessor), Arc::new(RwLock::new(SharedClusterState::non_cluster())))
}

#[derive(Debug)]
pub struct NoOpConsensusProcessor;

impl ConsensusProcessor for NoOpConsensusProcessor {

    fn peer_connection_established(&mut self, peer_id: FloInstanceId, connection_id: ConnectionId) {
        unimplemented!()
    }
    fn outgoing_connection_failed(&mut self, address: SocketAddr) {
        unimplemented!()
    }
}
