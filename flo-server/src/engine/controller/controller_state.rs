use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::collections::HashMap;
use std::io;

use event::EventCounter;
use engine::ConnectionId;
use engine::controller::{ConnectionRef, SystemEvent};
use engine::event_stream::{EventStreamRef, EventStreamRefMut, EventStreamOptions};
use engine::event_stream::partition::{SegmentNum, IndexEntry, PersistentEvent, PartitionReader};
use engine::event_stream::partition::controller::PartitionImpl;


pub trait ControllerState {
    fn add_connection(&mut self, connection: ConnectionRef);
    fn remove_connection(&mut self, connection_id: ConnectionId);
    fn get_connection(&self, connection_id: ConnectionId) -> Option<&ConnectionRef>;

    fn set_commit_index(&mut self, new_index: EventCounter);
    fn get_system_commit_index(&self) -> EventCounter;
    fn get_system_event(&mut self, event_counter: EventCounter) -> Option<io::Result<SystemEvent<PersistentEvent>>>;
    fn get_next_entry(&self, event_counter: EventCounter) -> Option<IndexEntry>;
    fn get_current_file_offset(&self) -> (SegmentNum, usize);
}

pub struct ControllerStateImpl {
    /// Shared references to all event streams in the system
    pub shared_event_stream_refs: Arc<Mutex<HashMap<String, EventStreamRef>>>,

    /// Unique mutable references to every event stream in the system
    pub event_streams: HashMap<String, EventStreamRefMut>,

    /// used as defaults when creating new event streams
    pub default_stream_options: EventStreamOptions,

    /// directory in which all event stream data is stored
    pub storage_dir: PathBuf,

    /// the partition that persists system events. Used as the RAFT log
    pub system_partition: PartitionImpl,

    /// a reader for the system partition, since we'll occasionally need to look up events
    pub system_partition_reader: PartitionReader,

    /// Stores all of the active connections with this instance
    pub all_connections: HashMap<ConnectionId, ConnectionRef>,
}

impl ControllerStateImpl {
    pub fn new(mut system_partition: PartitionImpl,
               event_streams: HashMap<String, EventStreamRefMut>,
               shared_event_stream_refs: Arc<Mutex<HashMap<String, EventStreamRef>>>,
               storage_dir: PathBuf,
               default_stream_options: EventStreamOptions) -> ControllerStateImpl {

        let system_partition_reader = system_partition.create_reader(0, ::engine::event_stream::partition::EventFilter::All, 0);

        ControllerStateImpl {
            shared_event_stream_refs,
            event_streams,
            default_stream_options,
            storage_dir,
            system_partition,
            system_partition_reader,
            all_connections: HashMap::new(),
        }
    }

    fn read_event(&mut self, entry: IndexEntry) -> io::Result<SystemEvent<PersistentEvent>> {
        self.system_partition_reader.set_to(entry.segment, entry.file_offset)?;
        let result = self.system_partition_reader.read_next_uncommitted().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Expected to find an event for {:?} but no event was read", entry))
        })?;
        result.and_then(|event| {
            SystemEvent::from_event(event).map_err(|des_err| {
                error!("Failed to deserialize system event data for {:?}: {:?}", entry, des_err);
                io::Error::new(io::ErrorKind::InvalidData, format!("Failed to deserialize system event data: {:?}", des_err))
            })
        })
    }
}

impl ControllerState for ControllerStateImpl {

    fn add_connection(&mut self, connection: ConnectionRef) {
        self.all_connections.insert(connection.connection_id, connection);
    }

    fn remove_connection(&mut self, connection_id: ConnectionId) {
        self.all_connections.remove(&connection_id);
    }

    fn get_connection(&self, connection_id: ConnectionId) -> Option<&ConnectionRef> {
        self.all_connections.get(&connection_id)
    }

    fn get_system_commit_index(&self) -> EventCounter {
        self.system_partition.get_commit_index()
    }

    fn set_commit_index(&mut self, new_index: EventCounter) {
        self.system_partition.set_commit_index(new_index);
    }

    fn get_system_event(&mut self, event_counter: EventCounter) -> Option<io::Result<SystemEvent<PersistentEvent>>> {
        self.get_next_entry(event_counter.saturating_sub(1)).and_then(|index_entry| {
            if index_entry.counter == event_counter {
                Some(self.read_event(index_entry))
            } else {
                None
            }
        })
    }

    fn get_next_entry(&self, previous: EventCounter) -> Option<IndexEntry> {
        self.system_partition.get_next_index_entry(previous)
    }

    fn get_current_file_offset(&self) -> (SegmentNum, usize) {
        self.system_partition.get_head_position()
    }
}



#[cfg(test)]
pub mod mock {
    use super::*;

    pub struct MockControllerState {
        pub commit_index: EventCounter,
        pub all_connections: HashMap<ConnectionId, ConnectionRef>,
    }

    impl MockControllerState {
        pub fn new() -> MockControllerState {
            MockControllerState::with_commit_index(0)
        }

        pub fn with_commit_index(index: EventCounter) -> MockControllerState {
            MockControllerState {
                commit_index: index,
                all_connections: HashMap::new(),
            }
        }
    }

    impl ControllerState for MockControllerState {
        fn add_connection(&mut self, connection: ConnectionRef) {
            self.all_connections.insert(connection.connection_id, connection);
        }
        fn remove_connection(&mut self, connection_id: ConnectionId) {
            self.all_connections.remove(&connection_id);
        }
        fn get_connection(&self, connection_id: ConnectionId) -> Option<&ConnectionRef> {
            self.all_connections.get(&connection_id)
        }
        fn get_system_commit_index(&self) -> EventCounter {
            unimplemented!()
        }

        fn get_system_event(&mut self, event_counter: EventCounter) -> Option<io::Result<SystemEvent<PersistentEvent>>> {
            unimplemented!()
        }
        fn get_next_entry(&self, event_counter: EventCounter) -> Option<IndexEntry> {
            unimplemented!()
        }

        fn get_current_file_offset(&self) -> (SegmentNum, usize) {
            unimplemented!()
        }
        fn set_commit_index(&mut self, new_index: EventCounter) {
            unimplemented!()
        }
    }

}
