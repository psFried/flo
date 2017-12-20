mod persistent;

use std::io;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use std::time::{Instant};
use std::path::Path;

use event::EventCounter;
use engine::ConnectionId;
use protocol::FloInstanceId;
use atomics::{AtomicBoolWriter, AtomicBoolReader};
use super::ClusterOptions;
use super::peer_connection::PeerSystemConnection;

pub use self::persistent::{FilePersistedState, PersistentClusterState};

pub trait ConsensusProcessor: Send {
    fn system_primary_address_reader(&self) -> Arc<RwLock<Option<SocketAddr>>>;
    fn system_primary_status_reader(&self) -> AtomicBoolReader;
    fn peer_connection_established(&mut self, peer_id: FloInstanceId, connection_id: ConnectionId);
}

#[derive(Debug)]
pub struct ClusterState {
    primary_status_writer: AtomicBoolWriter,
    starting_peer_addresses: Vec<SocketAddr>,
    last_applied: EventCounter,
    persistent: FilePersistedState,
    system_partion_primary_address: SystemPrimaryAddressRef,
    shared: Arc<RwLock<SharedClusterState>>,
}

impl ConsensusProcessor for ClusterState {

    fn peer_connection_established(&mut self, peer_id: FloInstanceId, connection_id: ConnectionId) {
        unimplemented!()
    }
    fn system_primary_status_reader(&self) -> AtomicBoolReader {
        self.primary_status_writer.reader()
    }
    fn system_primary_address_reader(&self) -> Arc<RwLock<Option<SocketAddr>>> {
        self.system_partion_primary_address.clone()
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
    fn non_cluster() -> SharedClusterState {
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


impl ClusterState {

    pub fn system_primary_status_reader(&self) -> AtomicBoolReader {
        self.primary_status_writer.reader()
    }

    pub fn system_primary_address_reader(&self) -> SystemPrimaryAddressRef {
        self.system_partion_primary_address.clone()
    }
}

pub type SystemPrimaryAddressRef = Arc<RwLock<Option<SocketAddr>>>;

pub type ClusterStateReader = Arc<RwLock<SharedClusterState>>;


pub fn init_consensus_processor(storage_dir: &Path, options: Option<ClusterOptions>) -> io::Result<(Box<ConsensusProcessor>, ClusterStateReader)> {
    let start_as_primary = options.is_none();
    if let Some(options) = options {
        debug!("Initializing in clustered mode, starting as secondary");
        init_cluster_state(storage_dir, options)
    } else {
        debug!("Initializing in non-cluster mode, system stream will always be considered primary");
        Ok(init_no_op_consensus_processor())
    }
}

fn init_cluster_state(storage_dir: &Path, options: ClusterOptions) -> io::Result<(Box<ConsensusProcessor>, ClusterStateReader)> {
    let path = storage_dir.join("cluster-state");
    let persistent_state = FilePersistedState::initialize(path)?; // early return if this fails

    let ClusterOptions{ this_instance_address, peer_addresses, event_loop_handles } = options;

    let shared_state = persistent_state.initialize_shared_state(Some(this_instance_address));
    let shared_state_ref = Arc::new(RwLock::new(shared_state));

    let state = ClusterState {
        primary_status_writer: AtomicBoolWriter::with_value(false),
        last_applied: 0,
        starting_peer_addresses: peer_addresses,
        persistent: persistent_state,
        system_partion_primary_address: Arc::new(RwLock::new(None)),
        shared: shared_state_ref.clone(),
    };
    Ok((Box::new(state), shared_state_ref))
}

fn init_no_op_consensus_processor() -> (Box<ConsensusProcessor>, ClusterStateReader) {
    (Box::new(NoOpConsensusProcessor::new()), Arc::new(RwLock::new(SharedClusterState::non_cluster())))
}

#[derive(Debug)]
pub struct NoOpConsensusProcessor {
    primary: AtomicBoolWriter,
    primary_address: Arc<RwLock<Option<SocketAddr>>>
}

impl NoOpConsensusProcessor {
    fn new() -> NoOpConsensusProcessor {
        NoOpConsensusProcessor {
            primary: AtomicBoolWriter::with_value(true),
            primary_address: Arc::new(RwLock::new(None)),
        }
    }
}

impl ConsensusProcessor for NoOpConsensusProcessor {

    fn peer_connection_established(&mut self, peer_id: FloInstanceId, connection_id: ConnectionId) {
        unimplemented!()
    }
    fn system_primary_status_reader(&self) -> AtomicBoolReader {
        self.primary.reader()
    }
    fn system_primary_address_reader(&self) -> Arc<RwLock<Option<SocketAddr>>> {
        self.primary_address.clone()
    }
}
