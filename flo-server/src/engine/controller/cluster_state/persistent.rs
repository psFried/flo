use std::net::SocketAddr;
use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom};

use protocol::{FloInstanceId, Term};
use engine::controller::controller_messages::Peer;
use super::{ClusterOptions, SharedClusterState};

/// Holds all the cluster state that we want to survive a reboot.
/// We always persist the `FloInstanceId` because we prefer that to be stable across reboots. We do _not_ want to persist
/// the `SocketAddr` for the server, though, since that may well change after a restart, depending on environment.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PersistentClusterState {
    pub current_term: Term,
    #[serde(with = "OptInstanceIdRemote")]
    pub voted_for: Option<FloInstanceId>,
    #[serde(with = "InstanceIdRemote")]
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
#[serde(remote = "FloInstanceId")]
pub struct InstanceIdRemote{
    #[serde(getter = "FloInstanceId::as_bytes")]
    bytes: [u8; 8]
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
#[serde(remote = "Option<FloInstanceId>")]
pub struct OptInstanceIdRemote{
    #[serde(getter = "id_as_bytes")]
    bytes: [u8; 8]
}

fn id_as_bytes(id: &Option<FloInstanceId>) -> [u8; 8] {
    id.map(|i| i.as_bytes()).unwrap_or([0; 8])
}

impl Into<Option<FloInstanceId>> for OptInstanceIdRemote {
    fn into(self) -> Option<FloInstanceId> {
        let id = FloInstanceId::from_bytes(&self.bytes[..]);
        if id == FloInstanceId::null() {
            None
        } else {
            Some(id)
        }
    }
}

impl Into<FloInstanceId> for InstanceIdRemote {
    fn into(self) -> FloInstanceId {
        FloInstanceId::from_bytes(&self.bytes[..])
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

        let (file, state) = if path.exists() {
            let mut file = OpenOptions::new().write(true).read(true).open(&path)?; // early return on failure
            let state = ::serde_json::from_reader(&mut file).map_err(|des_err| {
                error!("Failed to read persistent cluster state: {:?}", des_err);
                io::Error::new(io::ErrorKind::InvalidData, format!("Deserialization error: {}", des_err))
            })?; // early return if this fails
            (file, state)
        } else {
            let file = OpenOptions::new().write(true).read(true).create(true).open(&path)?; // early return on failure
            let state = PersistentClusterState {
                current_term: 0,
                voted_for: None,
                this_instance_id: FloInstanceId::generate_new(),
                cluster_members: Vec::new(),
            };
            info!("Initialized brand new state: {:?}", state);
            (file, state)
        };

        Ok(FilePersistedState {
            file,
            path,
            state
        })
    }

    pub fn modify<F>(&mut self, fun: F) -> Result<(), io::Error> where F: Fn(&mut PersistentClusterState) {
        fun(&mut self.state);
        self.flush()
    }

    pub fn flush(&mut self) -> io::Result<()> {
        use serde_json::to_writer_pretty;
        let FilePersistedState {ref mut file, ref state, ..} = *self;
        // all the early returns
        file.seek(SeekFrom::Start(0))?;
        to_writer_pretty(&mut *file, state)?;

        // truncates the file in case the modified data is shorter than the old data, so there won't be invalid garbage at the end
        let position = file.seek(SeekFrom::Current(0))?;
        file.set_len(position)?;

        /*
        It's debatable whether this is really necessary. _technically_, it's possible that an instance could cast a vote, have
        a power failure that causes unsynced data to be lost, then power on and vot again in the same term. This is the reason
        that Raft recommends persisting this information on disk in the first place, to prevent an instance from voting multiple
        times in one term. Given an election timeout of ~300 milliseconds, though, it seems pretty unlikely for a server to
        power cycle and vote twice, though. If we only need the persistent data to survive a restart of the flo process, then
        the sync probably isn't necessary. Anyway, we'll play it safe for now until we determine that it's actually causing a
        performance problem.
        */
        file.sync_data()
    }
}

impl ::std::ops::Deref for FilePersistedState {
    type Target = PersistentClusterState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempdir::TempDir;
    use test_utils::addr;

    #[test]
    fn modifying_state_persists_changes() {
        use std::ops::Deref;

        let temp = TempDir::new("persistent_state_test").unwrap();
        let path = temp.path().join("test_cluster_state");

        let mut subject = FilePersistedState::initialize(path.clone()).unwrap();
        subject.modify(|state| {
            state.current_term = 9;
            state.cluster_members = vec![
                Peer {
                    id: FloInstanceId::generate_new(),
                    address: addr("127.0.0.1:3456")
                },
                Peer {
                    id: FloInstanceId::generate_new(),
                    address: addr("[2001:873::1]:3000")
                },
                Peer {
                    id: FloInstanceId::generate_new(),
                    address: addr("127.0.0.1:456")
                }
            ];
        }).unwrap();

        let subject2 = FilePersistedState::initialize(path.clone()).unwrap();
        assert_eq!(subject.deref(), subject2.deref());

        // remove some data here. This will cause the second init to fail if we don't truncate the file
        subject.modify(|state| {
            state.cluster_members.pop();
            state.cluster_members.pop();
        }).unwrap();

        let subject2 = FilePersistedState::initialize(path.clone()).unwrap();
        assert_eq!(subject.deref(), subject2.deref());
    }

}
