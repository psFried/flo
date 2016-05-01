use super::Event;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::io::{self, Read};

const EVENTS_FILE_NAME: &'static str = "events.json";

pub struct PersistenceError;
pub type PersistenceResult = Result<(), PersistenceError>;

pub trait EventStore {

    fn store(&mut self, event: &Event) -> PersistenceResult;
}


pub struct FileSystemEventStore {
    persistence_dir: PathBuf,
    events_file: File,
}

impl FileSystemEventStore {

    pub fn new(persistence_dir: PathBuf) -> FileSystemEventStore {
        let file = File::create(persistence_dir.join(EVENTS_FILE_NAME)).unwrap();

        FileSystemEventStore {
            persistence_dir: persistence_dir,
            events_file: file,
        }
    }
}

impl EventStore for FileSystemEventStore {

    fn store(&mut self, event: &Event) -> PersistenceResult {
        ::serde_json::to_writer(&mut self.events_file, event)
            .map_err(|e| PersistenceError)
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use tempdir::TempDir;
    use ::{Event, to_event};
    use std::fs::File;
    use std::io::Read;
    use serde_json::{self, StreamDeserializer};

    #[test]
    fn multiple_events_are_saved_sequentially() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());

        let event1 = to_event(r#"{"myKey": "one"}"#).unwrap();
        store.store(&event1);

        let event2 = to_event(r#"{"myKey": "one"}"#).unwrap();
        store.store(&event2);

        let mut file_path = temp_dir.path().join("events.json");
        let mut file = File::open(file_path).unwrap();

        let mut bytes = file.bytes();
        let mut deser = StreamDeserializer::new(bytes);
        let saved = deser.map(Result::unwrap).collect::<Vec<Event>>();

        assert_eq!(2, saved.len());
        assert_eq!(event1, saved[0]);
        assert_eq!(event2, saved[1]);
    }

    #[test]
    fn event_is_saved_to_a_file_within_the_persistence_directory() {
        let temp_dir = TempDir::new("flo-persist-test").unwrap();
        let mut store = FileSystemEventStore::new(temp_dir.path().to_path_buf());

        let event = to_event(r#"{"myKey": "myVal"}"#).unwrap();
        store.store(&event);

        let mut file_path = temp_dir.path().join("events.json");
        let mut file = File::open(file_path).unwrap();

        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        let saved_event = to_event(&contents).unwrap();
        assert_eq!(event, saved_event);
    }
}
