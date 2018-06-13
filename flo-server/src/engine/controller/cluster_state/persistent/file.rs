use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom};

use super::PersistentClusterState;


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
            let state = PersistentClusterState::generate_new();
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
    use engine::controller::Peer;
    use tempdir::TempDir;
    use test_utils::addr;
    use protocol::flo_instance_id;

    #[test]
    fn modifying_state_persists_changes() {
        use std::ops::Deref;

        let temp = TempDir::new("persistent_state_test").unwrap();
        let path = temp.path().join("test_cluster_state");

        let mut subject = FilePersistedState::initialize(path.clone()).unwrap();
        subject.modify(|state| {
            state.current_term = 9;
            state.add_peer(&Peer { id: flo_instance_id::generate_new(), address: addr("127.0.0.1:3456") });
            state.add_peer(&Peer { id: flo_instance_id::generate_new(), address: addr("[2001:873::1]:3000") });
            state.add_peer(&Peer { id: flo_instance_id::generate_new(), address: addr("127.0.0.1:456") });
        }).unwrap();

        let subject2 = FilePersistedState::initialize(path.clone()).unwrap();
        assert_eq!(subject.deref(), subject2.deref());

        // remove some data here. This will cause the second init to fail if we don't truncate the file
        subject.modify(|state| {
            state.cluster_members.clear();
        }).unwrap();

        let subject2 = FilePersistedState::initialize(path.clone()).unwrap();
        assert_eq!(subject.deref(), subject2.deref());
    }

}
