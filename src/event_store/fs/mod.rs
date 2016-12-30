mod writer;
mod reader;

pub const DATA_FILE_NAME: &'static str = "events";
pub const FLO_EVT: &'static str = "FLO_EVT\n";

pub use self::writer::FSEventWriter;
pub use self::reader::{FSEventReader, FSEventIter};
use super::{StorageEngine, StorageEngineOptions, EventWriter, EventReader};
use event_store::index::EventIndex;

use std::path::{Path, PathBuf};
use std::fs::File;
use std::io;

pub struct FSStorageEngine;

fn initialize_index(storage_opts: &StorageEngineOptions) -> Result<EventIndex, io::Error> {

    unimplemented!()
}

impl StorageEngine for FSStorageEngine {
    type Writer = FSEventWriter;
    type Reader = FSEventReader;

    fn initialize(options: StorageEngineOptions) -> Result<(Self::Writer, Self::Reader), io::Error> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, RwLock};
    use super::*;
    use event_store::{EventReader, EventWriter, StorageEngineOptions};
    use event_store::index::EventIndex;
    use flo_event::{FloEventId, FloEvent, OwnedFloEvent};

    use tempdir::TempDir;

    #[test]
    fn index_is_initialized_from_preexisting_events() {
        let storage_dir = TempDir::new("events_are_written_and_read_from_preexisting_directory").unwrap();
        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            max_events: 20,
        };

        let event1 = OwnedFloEvent::new(FloEventId::new(1, 1), "/foo/bar".to_owned(), "first event data".as_bytes().to_owned());
        let event2 = OwnedFloEvent::new(FloEventId::new(1, 2), "/nacho/cheese".to_owned(), "second event data".as_bytes().to_owned());
        let event3 = OwnedFloEvent::new(FloEventId::new(1, 3), "/smalls/yourekillinme".to_owned(), "third event data".as_bytes().to_owned());

        {
            let index = Arc::new(RwLock::new(EventIndex::new(20)));
            let mut writer = FSEventWriter::initialize(index.clone(), &storage_opts).expect("Failed to create event writer");

            writer.store(&event1).expect("Failed to store event 1");
            writer.store(&event2).expect("Failed to store event 2");
            writer.store(&event3).expect("Failed to store event 3");
        }

        let result = initialize_index(&storage_opts).expect("failed to inialize event index");

    }

    #[test]
    fn events_are_stored_and_read_starting_in_the_middle_with_fresh_directory() {
        let event1 = OwnedFloEvent::new(FloEventId::new(1, 1), "/foo/bar".to_owned(), "first event data".as_bytes().to_owned());
        let event2 = OwnedFloEvent::new(FloEventId::new(1, 2), "/nacho/cheese".to_owned(), "second event data".as_bytes().to_owned());
        let event3 = OwnedFloEvent::new(FloEventId::new(1, 3), "/smalls/yourekillinme".to_owned(), "third event data".as_bytes().to_owned());

        let storage_dir = TempDir::new("events_are_stored_and_read_starting_in_the_middle_with_fresh_directory").unwrap();
        let index = Arc::new(RwLock::new(EventIndex::new(20)));
        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            max_events: 20,
        };
        let mut writer = FSEventWriter::initialize(index.clone(), &storage_opts).expect("Failed to create event writer");

        writer.store(&event1).expect("Failed to store event 1");
        writer.store(&event2).expect("Failed to store event 2");
        writer.store(&event3).expect("Failed to store event 3");

        let mut reader = FSEventReader::initialize(index, &storage_opts).expect("Failed to create event reader");

        let mut iter = reader.load_range(FloEventId::new(1, 1), 1);
        let result = iter.next().unwrap().expect("Expected event2, got error");
        assert_eq!(event2, result);

        assert!(iter.next().is_none());
    }

    #[test]
    fn events_are_stored_and_read_with_fresh_directory() {
        let event1 = OwnedFloEvent::new(FloEventId::new(1, 1), "/foo/bar".to_owned(), "first event data".as_bytes().to_owned());
        let event2 = OwnedFloEvent::new(FloEventId::new(1, 2), "/nacho/cheese".to_owned(), "second event data".as_bytes().to_owned());
        let event3 = OwnedFloEvent::new(FloEventId::new(1, 3), "/smalls/yourekillinme".to_owned(), "third event data".as_bytes().to_owned());

        let storage_dir = TempDir::new("events_are_stored_and_read_with_fresh_directory").unwrap();
        let index = Arc::new(RwLock::new(EventIndex::new(20)));
        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            max_events: 20,
        };
        let mut writer = FSEventWriter::initialize(index.clone(), &storage_opts).expect("Failed to create event writer");

        writer.store(&event1).expect("Failed to store event 1");
        writer.store(&event2).expect("Failed to store event 2");
        writer.store(&event3).expect("Failed to store event 3");

        let mut reader = FSEventReader::initialize(index, &storage_opts).expect("Failed to create event reader");

        let mut iter = reader.load_range(FloEventId::zero(), 999999);
        let result = iter.next().unwrap().expect("Expected event1, got error");
        assert_eq!(event1, result);

        let result = iter.next().unwrap().expect("Expected event2, got error");
        assert_eq!(event2, result);

        let result = iter.next().unwrap().expect("Expected event3, got error");
        assert_eq!(event3, result);

        assert!(iter.next().is_none());
    }

    #[test]
    fn event_is_serialized_and_deserialized() {
        use std::io::Cursor;
        let event = OwnedFloEvent::new(FloEventId::new(1, 1), "/foo/bar".to_owned(), "event data".as_bytes().to_owned());

        let mut buffer = Vec::new();
        let size = super::writer::write_event(&mut buffer, &event).expect("Failed to write event");
        println!("len: {}, size: {}, buffer: {:?}", buffer.len(), size, buffer);
        let mut reader = Cursor::new(&buffer[..(size as usize)]);

        let result = super::reader::read_event(&mut reader).expect("failed to read event");
        assert_eq!(event, result);
    }

}
