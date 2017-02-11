mod writer;
mod reader;

pub const DATA_FILE_NAME: &'static str = "events";
pub const FLO_EVT: &'static str = "FLO_EVT\n";

pub use self::writer::FSEventWriter;
pub use self::reader::{FSEventReader, FSEventIter};
use super::{StorageEngine, StorageEngineOptions};
use engine::event_store::index::{EventIndex, IndexEntry};
use engine::version_vec::VersionVector;
use flo_event::{FloEvent};

use std::sync::{Arc, RwLock};
use std::path::PathBuf;
use std::io;

pub struct FSStorageEngine;

pub fn total_size_on_disk<E: FloEvent>(event: &E) -> u64 {
    8 +             // FLO_EVT\n
            4 +     // total data length
            10 +    // event id
            10 +    // parent event id
            8 +     // timestamp
            4 +     // namespace length
            event.namespace().len() as u64 + // length of the actual namespace
            4 +     // data length field
            event.data_len() as u64 //the actual event data
}

fn events_file(storage_opts: &StorageEngineOptions) -> PathBuf {
    let mut dir = storage_opts.storage_dir.as_path().join(&storage_opts.root_namespace);
    dir.push(DATA_FILE_NAME);
    dir
}

fn init(storage_opts: &StorageEngineOptions) -> Result<(EventIndex, VersionVector), io::Error> {
    let events_file = events_file(storage_opts);

    if events_file.exists() {
        debug!("initializing index from file: {:?}", events_file);
        FSEventIter::initialize(0, ::std::usize::MAX, &events_file).and_then(|event_iter| {
            build_index(storage_opts.max_events, event_iter)
        })
    } else {
        debug!("No existing events at {:?}, creating new index", events_file);
        Ok((EventIndex::new(storage_opts.max_events), VersionVector::new()))
    }

}

fn build_index(max_events: usize, iter: FSEventIter) -> Result<(EventIndex, VersionVector), io::Error> {
    let mut index = EventIndex::new(max_events);
    let mut offset = 0;
    let mut event_count = 0;

    let mut version_vec = VersionVector::new();

    for result in iter {
        match result {
            Ok(event) => {
                version_vec.update(*event.id()).map_err(|str_err| {
                    io::Error::new(io::ErrorKind::InvalidData, str_err)
                })?; // early return if event counters are somehow out of order

                index.add(IndexEntry::new(*event.id(), offset)); //don't care about possible evictions here
                offset += total_size_on_disk(&event);
                event_count += 1;
            },
            Err(err) => return Err(err)
        }
    }
    debug!("Finished building index, iterated {} events and {} bytes", event_count, offset);
    Ok((index, version_vec))
}

impl StorageEngine for FSStorageEngine {
    type Writer = FSEventWriter;
    type Reader = FSEventReader;

    fn initialize(options: StorageEngineOptions) -> Result<(Self::Writer, Self::Reader, VersionVector), io::Error> {
        init(&options).and_then(|(index, version_vector)| {
            let index = Arc::new(RwLock::new(index));

            FSEventWriter::initialize(index.clone(), &options).and_then(|writer| {
                FSEventReader::initialize(index, &options).map(|reader| {
                    (writer, reader, version_vector)
                })
            })
        })
    }
}


#[cfg(test)]
mod test {
    use std::sync::{Arc, RwLock};
    use super::*;
    use engine::event_store::{EventReader, EventWriter, StorageEngineOptions};
    use engine::event_store::index::EventIndex;
    use flo_event::{FloEventId, OwnedFloEvent, Timestamp};
    use std::io::Cursor;

    use tempdir::TempDir;

    fn event_time() -> Timestamp {
        ::time::from_millis_since_epoch(12345)
    }

    #[test]
    fn storage_engine_initialized_from_preexisting_events() {
        let storage_dir = TempDir::new("events_are_written_and_read_from_preexisting_directory").unwrap();
        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            max_events: 20,
        };

        let event1 = OwnedFloEvent::new(FloEventId::new(1, 1), None, event_time(), "/foo/bar".to_owned(), "first event data".as_bytes().to_owned());
        let event2 = OwnedFloEvent::new(FloEventId::new(2, 2), None, event_time(), "/nacho/cheese".to_owned(), "second event data".as_bytes().to_owned());
        let event3 = OwnedFloEvent::new(FloEventId::new(2, 3), None, event_time(), "/smalls/yourekillinme".to_owned(), "third event data".as_bytes().to_owned());

        {
            let index = Arc::new(RwLock::new(EventIndex::new(20)));
            let mut writer = FSEventWriter::initialize(index.clone(), &storage_opts).expect("Failed to create event writer");

            writer.store(&event1).expect("Failed to store event 1");
            writer.store(&event2).expect("Failed to store event 2");
            writer.store(&event3).expect("Failed to store event 3");
        }

        let (mut writer, mut reader, version_vec) = FSStorageEngine::initialize(storage_opts).expect("failed to initialize storage engine");

        let event4 = OwnedFloEvent::new(FloEventId::new(1, 4), None, event_time(), "/yolo".to_owned(), "fourth event data".as_bytes().to_owned());
        writer.store(&event4).unwrap();

        let mut event_iter = reader.load_range(FloEventId::new(2, 2), 55);
        let result = event_iter.next().expect("expected result to be Some").expect("failed to read event 3");
        assert_eq!(event3, result);

        let result = event_iter.next().expect("expected result to be Some").expect("failed to read event 4");
        assert_eq!(event4, result);

        assert!(event_iter.next().is_none());

        // version vec still has counter of 1 from when version vec was initialized
        assert_eq!(1, version_vec.get(1));
        assert_eq!(3, version_vec.get(2));
    }

    #[test]
    fn event_size_on_disk_is_computed_correctly() {
        let event = OwnedFloEvent::new(FloEventId::new(9, 44), None, event_time(), "/foo/bar".to_owned(), "something happened".as_bytes().to_owned());
        let mut buffer = Vec::new();
        let size = super::writer::write_event(&mut buffer, &event).expect("Failed to write event");
        assert_eq!(buffer.len() as u64, size);

        let size = total_size_on_disk(&event);
        assert_eq!(buffer.len() as u64, size);
    }

    #[test]
    fn event_header_is_read() {
        let event_id = FloEventId::new(9, 44);
        let parent_id = Some(FloEventId::new(1, 2));
        let timestamp = event_time();
        let namespace = "/foo/bar";

        let event = OwnedFloEvent::new(event_id, parent_id, event_time(), namespace.to_owned(), "something happened".as_bytes().to_owned());
        let mut buffer = Vec::new();
        super::writer::write_event(&mut buffer, &event).expect("Failed to write event");

        let header = super::reader::read_header(&mut Cursor::new(buffer)).expect("Failed to read header");
        assert_eq!(event_id, header.event_id());
        assert_eq!(parent_id, header.parent_id());
        assert_eq!(timestamp, header.timestamp());
        assert_eq!(namespace.len() as u32, header.namespace_length)
    }

    #[test]
    fn events_are_stored_and_read_starting_in_the_middle_with_fresh_directory() {
        let event1 = OwnedFloEvent::new(FloEventId::new(1, 1), None, event_time(), "/foo/bar".to_owned(), "first event data".as_bytes().to_owned());
        let event2 = OwnedFloEvent::new(FloEventId::new(1, 2), None, event_time(), "/nacho/cheese".to_owned(), "second event data".as_bytes().to_owned());
        let event3 = OwnedFloEvent::new(FloEventId::new(1, 3), None, event_time(), "/smalls/yourekillinme".to_owned(), "third event data".as_bytes().to_owned());

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
        let event1 = OwnedFloEvent::new(FloEventId::new(1, 1), None, event_time(), "/foo/bar".to_owned(), "first event data".as_bytes().to_owned());
        let event2 = OwnedFloEvent::new(FloEventId::new(1, 2), None, event_time(), "/nacho/cheese".to_owned(), "second event data".as_bytes().to_owned());
        let event3 = OwnedFloEvent::new(FloEventId::new(1, 3), None, event_time(), "/smalls/yourekillinme".to_owned(), "third event data".as_bytes().to_owned());

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
        let event = OwnedFloEvent::new(FloEventId::new(1, 1), Some(FloEventId::new(34, 56)), event_time(), "/foo/bar".to_owned(), "event data".as_bytes().to_owned());

        let mut buffer = Vec::new();
        let size = super::writer::write_event(&mut buffer, &event).expect("Failed to write event");
        println!("len: {}, size: {}, buffer: {:?}", buffer.len(), size, buffer);
        let mut reader = Cursor::new(&buffer[..(size as usize)]);

        let result = super::reader::read_event(&mut reader).expect("failed to read event");
        assert_eq!(event, result);
    }

}
