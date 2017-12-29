use std::net::SocketAddr;
use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek};

use protocol::{FloInstanceId, Term};
use engine::controller::controller_messages::Peer;
use super::{ClusterOptions, SharedClusterState};

/// Holds all the cluster state that we want to survive a reboot.
/// We always persist the `FloInstanceId` because we prefer that to be stable across reboots. We do _not_ want to persist
/// the `SocketAddr` for the server, though, since that may well change after a restart, depending on environment.
#[derive(Debug, PartialEq, Clone)]
pub struct PersistentClusterState {
    pub current_term: Term,
    pub voted_for: Option<FloInstanceId>,
    pub this_instance_id: FloInstanceId,
    pub cluster_members: Vec<Peer>,
}

impl PersistentClusterState {
    /// called during system startup to initialize the shared cluster state that will be available to all the connection handlers
    pub fn initialize_shared_state(&self, this_address: Option<SocketAddr>) -> SharedClusterState {

        SharedClusterState {
            this_instance_id: self.this_instance_id,
            this_address,
            system_primary: None, // we are still starting up, so we have no idea who's primary
            peers: self.cluster_members.clone(),
        }
    }
}


/// Placeholder for a wrapper struct that will take care of persisting the state as it changes
#[derive(Debug)]
pub struct FilePersistedState {
    file: File,
    path: PathBuf,
    state: PersistentClusterState,
}

impl FilePersistedState {
    pub fn initialize(path: PathBuf) -> io::Result<FilePersistedState> {
        let file = OpenOptions::new().write(true).read(true).create(true).open(&path)?; // early return on failure

        let state = PersistentClusterState {
            current_term: 0,
            voted_for: None,
            this_instance_id: FloInstanceId::generate_new(),
            cluster_members: Vec::new(),
        };
        debug!("Initialized {:?}", state);

        Ok(FilePersistedState {
            file,
            path,
            state
        })
    }
}

impl ::std::ops::Deref for FilePersistedState {
    type Target = PersistentClusterState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

