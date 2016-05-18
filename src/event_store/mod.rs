mod index;
mod file_reader;

use event::{EventId, Event};
use std::fs::File;
use std::io::{Seek, SeekFrom, Read, Write};
use std::path::{Path, PathBuf};
use lru_time_cache::LruCache;
use self::index::{RingIndex, Entry};
use self::file_reader::{FileReader, EventsFromDisk};

const EVENTS_FILE_NAME: &'static str = "events.json";
const MAX_CACHED_EVENTS: usize = 150;

pub const MAX_NUM_EVENTS: usize = 1_000_000;

pub struct PersistenceError;
pub type PersistenceResult = Result<EventId, PersistenceError>;

pub trait EventStore {

    fn store(&mut self, event: Event) -> PersistenceResult;

    fn get_event_greater_than(&mut self, event_id: EventId) -> Option<&mut Event>;

}


pub struct FileSystemEventStore {
    persistence_dir: PathBuf,
    events_file: File,
    index: RingIndex,
    event_cache: LruCache<EventId, Event>,
    current_file_position: u64,
    file_reader: FileReader,
}


impl FileSystemEventStore {

    pub fn new(persistence_dir: PathBuf) -> FileSystemEventStore {
        let file = File::create(persistence_dir.join(EVENTS_FILE_NAME)).unwrap();

        FileSystemEventStore {
            file_reader: FileReader::new(persistence_dir.join(EVENTS_FILE_NAME)),
            current_file_position: file.metadata().map(|md| md.len()).unwrap_or(0),
            events_file: file,
            index: RingIndex::new(1000, MAX_NUM_EVENTS),
            event_cache: LruCache::<EventId, Event>::with_capacity(MAX_CACHED_EVENTS),
            persistence_dir: persistence_dir,
        }
    }

    fn get_storage_file_path(&self) -> PathBuf {
        self.persistence_dir.join(EVENTS_FILE_NAME)
    }

    fn open_file_for_reading(&self) -> File {
        use std::fs::OpenOptions;

        OpenOptions::new().read(true).write(false)
                .open(self.get_storage_file_path())
                .unwrap()
    }


}

impl EventStore for FileSystemEventStore {

    fn store(&mut self, event: Event) -> PersistenceResult {
        debug!("Storing event: {:?}", &event.data);
        let mut evt = event;
        let event_id: EventId = evt.get_id();
        self.index.add(Entry::new(event_id, self.current_file_position));
        self.events_file.write(evt.get_raw_bytes());
        self.current_file_position += evt.get_raw_bytes().len() as u64;
        self.event_cache.insert(event_id, evt);
        Ok(event_id)
    }

    fn get_event_greater_than(&mut self, event_id: EventId) -> Option<&mut Event> {
        let FileSystemEventStore{ref mut index, ref mut event_cache, ref file_reader, ..} = *self;

        index.get_next_entry(event_id).and_then(move |entry| {
            if event_cache.contains_key(&entry.event_id) {
                trace!("returning cached event: {}", event_id);
                event_cache.get_mut(&entry.event_id)
            } else {
                match file_reader.read_from_offset(entry.offset).next() {
                    Some(Ok(event)) => {
                        trace!("event cache miss for event: {}, read from file", entry.event_id);
                        event_cache.insert(entry.event_id, event);
                        event_cache.get_mut(&entry.event_id)
                    },
                    _ => None
                }
            }

        })
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use tempdir::TempDir;
    use event::{Event, EventId, to_event, Json};
    use std::fs::File;
    use std::io::Read;
    use serde_json::StreamDeserializer;
	use serde_json::builder::ObjectBuilder;
    use event_store::index::Entry;

    #[test]
    fn storing_an_event_adds_its_starting_offset_to_the_index() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());

        let first_event_data = r#"{"id":1,"data":{"firstEventKey":"firstEventValue"}}"#;

        let event = Event::from_str(first_event_data).unwrap();
        store.store(event);
        let event = to_event(2, r#"{"secondEventKey": "secondEventValue"}"#).unwrap();
        store.store(event);

        let index_entry = store.index.get(2).unwrap();
        let expected_offset = first_event_data.as_bytes().len() as u64;
        assert_eq!(expected_offset, index_entry.offset);
    }

    #[test]
    fn storing_an_event_adds_its_event_id_to_the_index() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());

        let event_id: EventId = 3;
        let event = to_event(event_id, r#"{"myKey": "myValue"}"#).unwrap();

        store.store(event);

        let index_entry = store.index.get(event_id);
        assert!(index_entry.is_some());
        assert_eq!(index_entry.unwrap().offset, 0); // first entry written to file
    }

    #[test]
    fn get_next_event_returns_a_non_cached_event_stored_on_disk() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());

        for i in 1..11 {
            let event_json = ObjectBuilder::new().insert("myKey", i).unwrap();
            let event = Event::new(i, event_json);
            store.store(event);
            store.event_cache.remove(&i);
        }
        assert!(store.event_cache.is_empty());
        let result = store.get_event_greater_than(5);
        assert!(result.is_some());
        assert_eq!(6, result.unwrap().get_id());
    }

    #[test]
    fn get_next_event_returns_a_cached_event() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());
        let event_id: EventId = 3;
        let mut event = to_event(event_id, r#"{"myKey": "one"}"#).unwrap();

        store.event_cache.insert(event_id, event.clone());
        store.index.add(Entry::new(event_id, 9876));

        let result = store.get_event_greater_than(event_id - 1);
        assert_eq!(Some(&mut event), result);
    }

    #[test]
    fn events_are_put_into_the_cache_when_they_are_stored() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());

        let event = to_event(1, r#"{"myKey": "one"}"#).unwrap();
        store.store(event.clone());
        assert_eq!(Some(&event), store.event_cache.get(&1));
    }

    #[test]
    fn multiple_events_are_saved_sequentially() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());

        let event1 = to_event(1, r#"{"myKey": "one"}"#).unwrap();
        store.store(event1.clone());

        let event2 = to_event(2, r#"{"myKey": "one"}"#).unwrap();
        store.store(event2.clone());

        let file_path = temp_dir.path().join("events.json");
        let file = File::open(file_path).unwrap();

        let bytes = file.bytes();
        let deser = StreamDeserializer::new(bytes);
        let saved = deser.map(Result::unwrap)
                .map(Event::from_complete_json)
                .collect::<Vec<Event>>();

        assert_eq!(2, saved.len());
        assert_eq!(event1, saved[0]);
        assert_eq!(event2, saved[1]);
    }

    #[test]
    fn event_is_saved_to_a_file_within_the_persistence_directory() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());

        let event = to_event(1, r#"{"myKey": "myVal"}"#).unwrap();
        store.store(event.clone());

        let file_path = temp_dir.path().join("events.json");
        let mut file = File::open(file_path).unwrap();

        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        let saved_event = Event::from_str(&contents).unwrap();
        assert_eq!(event, saved_event);
    }
}
