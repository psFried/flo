mod persistent;

use std::io;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use std::time::{Instant};
use std::path::Path;

use event::EventCounter;
use protocol::FloInstanceId;
use atomics::{AtomicBoolWriter, AtomicBoolReader};
use super::ClusterOptions;

pub use self::persistent::{FilePersistedState, PersistentClusterState};


#[derive(Debug)]
pub struct ClusterState {
    primary_status_writer: AtomicBoolWriter,
    cluster_options: Option<ClusterOptions>,
    last_applied: EventCounter,
    persistent: FilePersistedState,
    system_partion_primary_address: SystemPrimaryAddressRef,
    shared: Arc<RwLock<SharedClusterState>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SharedClusterState {
    pub this_instance_id: FloInstanceId,
    pub this_address: Option<SocketAddr>,
    pub system_primary: Option<Peer>,
    pub peers: Vec<Peer>,
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

    pub fn reader(&self) -> ClusterStateReader {
        self.shared.clone()
    }
}

pub type SystemPrimaryAddressRef = Arc<RwLock<Option<SocketAddr>>>;

pub type ClusterStateReader = Arc<RwLock<SharedClusterState>>;


pub fn init_cluster_state(storage_dir: &Path, options: Option<ClusterOptions>) -> io::Result<ClusterState> {
    let path = storage_dir.join("cluster-state");
    let persistent_state = FilePersistedState::initialize(path)?; // early return if this fails
    let this_address = options.as_ref().map(|opts| opts.this_instance_address);
    let shared_state = persistent_state.initialize_shared_state(this_address);

    let start_as_primary = options.is_none();
    if start_as_primary {
        debug!("Initializing in non-cluster mode, system stream will always be considered primary");
    } else {
        debug!("Initializing in clustered mode, starting as secondary")
    }

    Ok(ClusterState {
        primary_status_writer: AtomicBoolWriter::with_value(start_as_primary),
        cluster_options: options,
        last_applied: 0,
        persistent: persistent_state,
        system_partion_primary_address: Arc::new(RwLock::new(None)),
        shared: Arc::new(RwLock::new(shared_state)),
    })
}

