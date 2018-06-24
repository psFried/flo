use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::collections::HashMap;
use std::io;
use tokio_core::reactor::Remote;

use protocol::{Term, FloInstanceId};
use event::{FloEvent, OwnedFloEvent, EventCounter, ActorId};
use engine::ConnectionId;
use engine::controller::{ConnectionRef, SystemEvent, SystemEventData};
use engine::event_stream::{EventStreamRef, EventStreamRefMut, EventStreamOptions, init_new_event_stream};
use engine::event_stream::partition::{SegmentNum, IndexEntry, PersistentEvent, PartitionReader, ReplicationResult};
use engine::event_stream::partition::controller::PartitionImpl;

#[derive(Debug, PartialEq)]
pub struct SystemEventRef {
    pub counter: EventCounter,
    pub data: SystemEventData
}

impl SystemEventRef {
    pub fn term(&self) -> Term {
        self.data.term
    }
}

impl <T: FloEvent> From<SystemEvent<T>> for SystemEventRef {
    fn from(evt: SystemEvent<T>) -> Self {
        let counter = evt.counter();
        SystemEventRef {
            counter,
            data: evt.deserialized_data
        }
    }
}

pub trait ControllerState {
    fn add_connection(&mut self, connection: ConnectionRef);
    fn remove_connection(&mut self, connection_id: ConnectionId);
    fn get_connection(&self, connection_id: ConnectionId) -> Option<&ConnectionRef>;

    fn set_partitions_writable(&mut self, partition_num: ActorId);
    fn create_event_stream(&mut self, options: EventStreamOptions) -> io::Result<()>;
    fn create_partitions(&mut self, partition_num: ActorId) -> io::Result<()>;

    fn get_last_uncommitted_event(&mut self) -> Option<io::Result<SystemEventRef>>;
    fn get_commit_index(&self) -> EventCounter;
    fn set_commit_index(&mut self, new_index: EventCounter);
    fn get_last_committed(&mut self) -> io::Result<(EventCounter, Term)>;
    fn get_next_event(&mut self, start_after: EventCounter) -> Option<io::Result<SystemEventRef>>;

    fn get_next_counter_and_term(&mut self, start_after: EventCounter) -> Option<io::Result<(EventCounter, Term)>> {
        self.get_next_event(start_after).map(|result| {
            result.map(|event| {
                (event.counter, event.data.term)
            })
        })
    }

    fn get_next_entry(&self, event_counter: EventCounter) -> Option<IndexEntry>;
    fn get_current_file_offset(&self) -> (SegmentNum, usize);

    fn add_system_replication_node(&mut self, peer: FloInstanceId);
    fn system_event_ack(&mut self, peer: FloInstanceId, event: EventCounter) -> Option<EventCounter>;

    /// Called only by the clusterState and only when this instance is a follower
    fn replicate_system_events(&mut self, events: &[OwnedFloEvent]) -> io::Result<ReplicationResult>;

    /// Called only by the clusterState and only when this instance is primary
    fn produce_system_event(&mut self, event: SystemEventData) -> io::Result<EventCounter>;
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

    remote: Remote,
}

impl ControllerStateImpl {
    pub fn new(mut system_partition: PartitionImpl,
               event_streams: HashMap<String, EventStreamRefMut>,
               shared_event_stream_refs: Arc<Mutex<HashMap<String, EventStreamRef>>>,
               storage_dir: PathBuf,
               default_stream_options: EventStreamOptions,
               remote: Remote) -> ControllerStateImpl {

        let system_partition_reader = system_partition.create_reader(0, ::engine::event_stream::partition::EventFilter::All, 0);

        ControllerStateImpl {
            remote,
            shared_event_stream_refs,
            event_streams,
            default_stream_options,
            storage_dir,
            system_partition,
            system_partition_reader,
            all_connections: HashMap::new(),
        }
    }

    fn add_event_stream(&mut self, new_stream: EventStreamRefMut) {
        let name = new_stream.get_name().to_owned();
        let shared_ref = new_stream.clone_ref();

        self.event_streams.insert(name.clone(), new_stream);
        let mut lock = self.shared_event_stream_refs.lock().unwrap();
        lock.insert(name, shared_ref);
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

    fn set_partitions_writable(&mut self, partition_num: ActorId) {
        for event_stream in self.event_streams.values_mut() {
            event_stream.set_writable_partition(partition_num);
        }
    }

    fn create_event_stream(&mut self, options: EventStreamOptions) -> Result<(), io::Error> {
        if self.event_streams.contains_key(&options.name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Event stream already exists"));
        }

        let storage_dir = self.storage_dir.clone();
        let remote = self.remote.clone();

        init_new_event_stream(storage_dir, options, remote, false).map(|stream| {
            self.add_event_stream(stream);
        })
    }

    fn create_partitions(&mut self, _partition_num: u16) -> Result<(), io::Error> {
        unimplemented!()
    }

    fn get_last_uncommitted_event(&mut self) -> Option<Result<SystemEventRef, io::Error>> {
        let counter = self.system_partition.get_highest_uncommitted_event();
        self.get_next_event(counter - 1)
    }

    fn get_commit_index(&self) -> EventCounter {
        self.system_partition.get_commit_index()
    }

    fn set_commit_index(&mut self, new_index: EventCounter) {
        self.system_partition.update_commit_index(new_index);
    }

    fn get_last_committed(&mut self) -> io::Result<(EventCounter, Term)> {
        let commit_index = self.system_partition.get_commit_index();
        if commit_index == 0 {
            return Ok((0, 0));
        }
        match self.get_next_counter_and_term(commit_index - 1) {
            Some(result) => {
                result
            }
            None => {
                error!("No SystemEvent was found for the commit index: {}. System partition is in an invalid state!", commit_index);
                Err(io::Error::new(io::ErrorKind::InvalidData, format!("Expected to have a SystemEvent at index: {} since that is the commit index!", commit_index)))
            }
        }
    }

    fn get_next_event(&mut self, start_after: EventCounter) -> Option<io::Result<SystemEventRef>> {
        self.get_next_entry(start_after.saturating_sub(1)).map(|index_entry| {
            self.read_event(index_entry).map(|system_event| system_event.into() )
        })
    }

    fn get_next_entry(&self, previous: EventCounter) -> Option<IndexEntry> {
        self.system_partition.get_next_index_entry(previous)
    }

    fn get_current_file_offset(&self) -> (SegmentNum, usize) {
        self.system_partition.get_head_position()
    }

    fn add_system_replication_node(&mut self, peer: FloInstanceId) {
        self.system_partition.add_replication_node(peer);
    }

    fn system_event_ack(&mut self, peer: FloInstanceId, event: EventCounter) -> Option<EventCounter> {
        self.system_partition.events_acknowledged(peer, event)
    }

    fn replicate_system_events(&mut self, events: &[OwnedFloEvent]) -> io::Result<ReplicationResult> {
        self.system_partition.replicate_events(0, events)
    }

    fn produce_system_event(&mut self, _event: SystemEventData) -> Result<u64, io::Error> {
        unimplemented!()
    }
}



#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::BTreeMap;
    use engine::controller::{SystemEventKind, Peer};

    #[derive(Debug)]
    pub struct MockControllerState {
        pub commit_index: EventCounter,
        pub all_connections: HashMap<ConnectionId, ConnectionRef>,
        pub system_events: BTreeMap<EventCounter, MockSystemEvent>,
        pub writable_partitions: Option<ActorId>,
    }

    impl MockControllerState {
        pub fn new() -> MockControllerState {
            MockControllerState {
                commit_index: 0,
                all_connections: HashMap::new(),
                system_events: BTreeMap::new(),
                writable_partitions: None,
            }
        }

        pub fn with_commit_index(mut self, index: EventCounter) -> MockControllerState {
            self.commit_index = index;
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

        pub fn add_new_mock_event(&mut self, counter: EventCounter, term: Term) {
            self.add_mock_event(MockSystemEvent::with_any_data(counter, term, SegmentNum::new(1), 77))
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
        fn set_partitions_writable(&mut self, partition_num: u16) {
            self.writable_partitions = Some(partition_num);
        }
        fn create_event_stream(&mut self, _options: EventStreamOptions) -> Result<(), io::Error> {
            unimplemented!()
        }
        fn create_partitions(&mut self, _partition_num: u16) -> Result<(), io::Error> {
            unimplemented!()
        }

        fn get_last_uncommitted_event(&mut self) -> Option<Result<SystemEventRef, io::Error>> {
            self.system_events.iter().next_back().map(|(id, mock_sys_event)| {
                Ok(SystemEventRef {
                    counter: *id,
                    data: mock_sys_event.data.clone()
                })
            })
        }


        fn get_commit_index(&self) -> EventCounter {
            self.commit_index
        }

        fn set_commit_index(&mut self, new_index: EventCounter) {
            self.commit_index = self.commit_index.max(new_index);
        }
        fn get_last_committed(&mut self) -> io::Result<(EventCounter, Term)> {
            let commit_idx = self.commit_index;
            self.system_events.get(&commit_idx).map(|sys| {
                (sys.id, sys.data.term)
            }).ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, format!("Test error because there is no stubbed event for the commit index: {}", commit_idx))
            })
        }
        fn get_next_event(&mut self, start_after: EventCounter) -> Option<io::Result<SystemEventRef>> {
            self.system_events.range((start_after + 1)..).next().map(|(id, sys)| {
                Ok(SystemEventRef {
                    counter: *id,
                    data: sys.data.clone(),
                })
            })
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

        fn add_system_replication_node(&mut self, _peer: FloInstanceId) {
            unimplemented!()
        }

        fn system_event_ack(&mut self, _peer: u64, _event: u64) -> Option<u64> {
            unimplemented!()
        }

        fn replicate_system_events(&mut self, events: &[OwnedFloEvent]) -> io::Result<ReplicationResult> {
            let segment_num = self.system_events.values().last().map(|e| e.segment).unwrap_or(SegmentNum::new(1));
            let mut file_offset = self.system_events.values().last().map(|e| e.file_offset).unwrap_or(0);
            let mut highest = 0;
            for event in events.iter() {
                let counter = event.id.event_counter;
                let system_event = SystemEvent::from_event(event).map_err(|se| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("invalid system event data: {}", se))
                })?;
                let data = system_event.deserialized_data;
                let mock = MockSystemEvent {
                    id: counter,
                    segment: segment_num,
                    data,
                    file_offset,
                };
                self.add_mock_event(mock);
                file_offset += 100;
                highest = highest.max(counter);
            }
            Ok(ReplicationResult {
                op_id: 0,
                success: true,
                highest_event_counter: highest,
            })
        }

        fn produce_system_event(&mut self, _event: SystemEventData) -> Result<u64, io::Error> {
            unimplemented!()
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct MockSystemEvent {
        pub id: EventCounter,
        pub data: SystemEventData,
        pub segment: SegmentNum,
        pub file_offset: usize,

    }

    impl MockSystemEvent {
        pub fn with_any_data(counter: EventCounter, term: Term, segment: SegmentNum, file_offset: usize) -> MockSystemEvent {
            use test_utils::addr;

            let data = SystemEventKind::NewClusterMemberJoining(Peer {
                id: 45678,
                address: addr("127.0.0.1:3000")
            });
            MockSystemEvent::with_data(counter, term, segment, file_offset, data)
        }

        pub fn with_data(counter: EventCounter, term: Term, segment: SegmentNum, file_offset: usize, data: SystemEventKind) -> MockSystemEvent {
            MockSystemEvent {
                id: counter,
                segment,
                file_offset,
                data: SystemEventData {
                    term,
                    kind: data
                }
            }
        }
    }

}
