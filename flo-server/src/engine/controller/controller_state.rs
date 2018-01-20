use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::collections::HashMap;
use std::io;

use protocol::Term;
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

    fn get_last_committed(&mut self) -> io::Result<(EventCounter, Term)>;
    fn get_next_event(&mut self, start_after: EventCounter) -> Option<io::Result<(EventCounter, Term)>>;

    fn set_commit_index(&mut self, new_index: EventCounter);
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

    fn set_commit_index(&mut self, new_index: EventCounter) {
        self.system_partition.set_commit_index(new_index);
    }

    fn get_next_entry(&self, previous: EventCounter) -> Option<IndexEntry> {
        self.system_partition.get_next_index_entry(previous)
    }

    fn get_current_file_offset(&self) -> (SegmentNum, usize) {
        self.system_partition.get_head_position()
    }

    fn get_last_committed(&mut self) -> io::Result<(EventCounter, Term)> {
        let start_after = self.system_partition.get_commit_index();
        if start_after == 0 {
            return Ok((0, 0));
        }
        match self.get_next_event(start_after) {
            Some(result) => result,
            None => {
                error!("No SystemEvent was found for the commit index: {}. System partition is in an invalid state!", start_after);
                Err(io::Error::new(io::ErrorKind::InvalidData, format!("Expected to have a SystemEvent at index: {} since that is the commit index!", start_after)))
            }
        }
    }

    fn get_next_event(&mut self, start_after: EventCounter) -> Option<io::Result<(EventCounter, Term)>> {
        self.get_next_entry(start_after.saturating_sub(1)).map(|index_entry| {
            self.read_event(index_entry).map(|system_event| {
                (start_after, system_event.term())
            })
        })
    }
}



#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Debug)]
    pub struct MockControllerState {
        pub commit_index: EventCounter,
        pub all_connections: HashMap<ConnectionId, ConnectionRef>,
        pub system_events: BTreeMap<EventCounter, MockSystemEvent>,
    }

    impl MockControllerState {
        pub fn new() -> MockControllerState {
            MockControllerState {
                commit_index: 0,
                all_connections: HashMap::new(),
                system_events: BTreeMap::new(),
            }
        }

        pub fn with_commit_index(mut self, index: EventCounter) -> MockControllerState {
            self.set_commit_index(index);
            self
        }

        pub fn with_connection(mut self, conn: ConnectionRef) -> Self {
            self.add_connection(conn);
            self
        }

        pub fn with_mocked_events(mut self, events: &[MockSystemEvent]) -> Self {
            for e in events {
                self.add_mock_event(e.clone());
            }
            self
        }

        pub fn add_mock_event(&mut self, event: MockSystemEvent) {
            self.system_events.insert(event.id, event);
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
        fn get_next_entry(&self, event_counter: EventCounter) -> Option<IndexEntry> {
            self.system_events.range((event_counter + 1)..).next().map(|(id, sys)| {
                IndexEntry::new(*id, sys.segment, sys.file_offset)
            })
        }

        fn get_current_file_offset(&self) -> (SegmentNum, usize) {
            self.system_events.values().last().map(|sys| {
                (sys.segment, sys.file_offset)
            }).unwrap_or((SegmentNum::new_unset(), 0))
        }
        fn set_commit_index(&mut self, new_index: EventCounter) {
            self.commit_index = new_index;
        }
        fn get_last_committed(&mut self) -> io::Result<(EventCounter, Term)> {
            let commit_idx = self.commit_index;
            self.system_events.get(&commit_idx).map(|sys| {
                (sys.id, sys.term)
            }).ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, format!("Test error because there is no stubbed event for the commit index: {}", commit_idx))
            })
        }
        fn get_next_event(&mut self, start_after: EventCounter) -> Option<io::Result<(EventCounter, Term)>> {
            self.system_events.range((start_after + 1)..).next().map(|(id, sys)| {
                Ok((*id, sys.term))
            })
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct MockSystemEvent {
        pub id: EventCounter,
        pub term: Term,
        pub segment: SegmentNum,
        pub file_offset: usize,
    }

}
