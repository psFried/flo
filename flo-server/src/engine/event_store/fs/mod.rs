mod writer;
mod reader;

pub const DATA_FILE_EXTENSION: &'static str = ".events";
pub const FLO_EVT: &'static str = "FLO_EVT\n";

// TODO: don't re-export these types
pub use self::writer::FSEventWriter;
pub use self::reader::file_reader::FSEventIter;
pub use self::reader::FSEventReader;

use super::{StorageEngine, StorageEngineOptions};
use engine::event_store::index::{EventIndex};
use event::{FloEvent, FloEventId, ActorId, VersionVector, Timestamp};
use byteorder::{ByteOrder, BigEndian};

use std::sync::{Arc, RwLock};
use std::path::{PathBuf, Path};
use std::io::{self, Write, Read};
use std::fs;
use std::ffi::OsStr;

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

fn get_events_filename(actor_id: ActorId) -> String {
    format!("{}{}", actor_id, DATA_FILE_EXTENSION)
}

fn get_events_file(partition_directory: &Path, actor_id: ActorId) -> PathBuf {
    partition_directory.join(get_events_filename(actor_id))
}

fn get_event_counter_file(partition_directory: &Path, actor_id: ActorId) -> PathBuf {
    partition_directory.join(format!("{}.counter", actor_id))
}

fn get_segment_directory(storage_dir: &Path, partition_number: u64) -> PathBuf {
    storage_dir.join(partition_number.to_string())
}

impl StorageEngine for FSStorageEngine {
    type Writer = FSEventWriter;
    type Reader = FSEventReader;

    fn initialize(options: StorageEngineOptions) -> Result<(Self::Writer, Self::Reader, VersionVector), io::Error> {
        info!("initializing storage engine in directory: {:?}", &options.storage_dir);
        let index = Arc::new(RwLock::new(EventIndex::new()));

        FSEventWriter::initialize(index.clone(), &options).and_then(|writer| {
            FSEventReader::initialize(index.clone(), &options).and_then(|reader| {
                index.read().map_err(|lock_err| {
                    io::Error::new(io::ErrorKind::Other, format!("failed to acquire index lock: {:?}", lock_err))
                }).map(|idx| {
                    let version_vec = idx.get_version_vector().clone();
                    (writer, reader, version_vec)
                })
            })
        })
    }
}

fn open_segment_start_time(segment_dir: &Path, create: bool) -> Result<fs::File, io::Error> {
    let path = segment_dir.join("segment_start");
    fs::OpenOptions::new().read(true).write(true).create(create).append(true).open(&path)
}


fn read_segment_start_time(segment_dir: &Path) -> Result<Option<Timestamp>, io::Error> {
    let mut info_file = match open_segment_start_time(segment_dir, false) {
        Err(err) => {
            if err.kind() == io::ErrorKind::NotFound {
                return Ok(None);
            } else {
                return Err(err);
            }
        }
        Ok(file) => file
    };
    let time_u64 = read_u64(&mut info_file)?;
    let timestamp = ::event::time::from_millis_since_epoch(time_u64);
    Ok(Some(timestamp))
}

fn write_u64<W: Write>(number: u64, writer: &mut W) -> Result<(), io::Error> {
    let mut buffer = [0u8; 8];
    BigEndian::write_u64(&mut buffer, number);
    writer.write_all(&buffer)
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64, io::Error> {
    let mut buffer = [0u8; 8];
    match reader.read_exact(&mut buffer) {
        Ok(()) => {
            Ok(BigEndian::read_u64(&buffer))
        }
        Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            // that's fine. Just return 0 since the file just didn't contain any data
            Ok(0)
        }
        Err(e) => Err(e)
    }
}

fn write_segment_start_time(segment_dir: &Path, start_timestamp: Timestamp) -> Result<(), io::Error> {
    let mut info_file = open_segment_start_time(segment_dir, true)?;
    let mut buffer = [0u8; 8];
    BigEndian::write_u64(&mut buffer, ::event::time::millis_since_epoch(start_timestamp));
    info_file.write_all(&buffer)
}


fn get_integer_value(filename: &OsStr) -> Option<u64> {
    filename.to_str().and_then(|utf8| utf8.parse::<u64>().ok())
}


fn determine_existing_actors(storage_dir: &Path) -> Result<Vec<ActorId>, io::Error> {
    let mut actors = Vec::new();
    for entry in fs::read_dir(storage_dir)? {
        let filename_os_str = entry?.file_name();

        if is_data_file(&filename_os_str) {
            debug!("Found events file: '{:?}'", filename_os_str);
            let actor_id = filename_os_str.to_str().and_then(|filename| {
                filename.split_terminator(DATA_FILE_EXTENSION).next()
            }).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, format!("The filename '{:?}' is invalid", filename_os_str))
            }).and_then(|id_str| {
                id_str.parse::<ActorId>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("The filename '{:?}' should start with an integer", filename_os_str))
                })
            })?;
            actors.push(actor_id);
        }
    }
    actors.sort();

    Ok(actors)
}

fn is_data_file(input: &OsStr) -> bool {
    input.to_string_lossy().ends_with(DATA_FILE_EXTENSION)
}

fn determine_existing_partitions(storage_dir: &Path) -> Result<Vec<u64>, io::Error> {
    let mut partitions = Vec::new();
    for entry in fs::read_dir(storage_dir)? {
        let entry = entry?; // early return again if we can't obtain the next entry
        let filename_os_str = entry.file_name();

        if let Some(partition_number) = get_integer_value(&filename_os_str) {
            partitions.push(partition_number);
        } else {
            warn!("Ignoring extra file in storage directory: {:?}", filename_os_str);
        }
    }
    partitions.sort();

    debug!("Initializing from existing partitions: {:?}", partitions);
    Ok(partitions)
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, RwLock};
    use super::*;
    use engine::event_store::{EventReader, EventWriter, StorageEngineOptions};
    use engine::event_store::index::EventIndex;
    use event::{time, FloEventId, OwnedFloEvent, Timestamp};
    use std::io::Cursor;
    use chrono::Duration;

    use tempdir::TempDir;

    fn event_time() -> Timestamp {
        time::from_millis_since_epoch(12345)
    }

    #[test]
    fn events_are_evicted() {
        let _ = ::env_logger::init();

        let storage_dir = TempDir::new("events_are_expired").unwrap();

        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            event_retention_duration: Duration::milliseconds(100),
            event_eviction_period: Duration::milliseconds(10)
        };

        fn event(time: u64, counter: u64) -> OwnedFloEvent {
            OwnedFloEvent::new(FloEventId::new(1, counter), None, time::from_millis_since_epoch(time), "foo".to_owned(), Vec::new())
        }

        let e1 = event(1, 1);
        let e2 = event(10, 2);
        let e3 = event(20, 3);
        let e4 = event(30, 4);

        let (mut writer, mut reader, _) = FSStorageEngine::initialize(storage_opts).expect("failed to initialize engine");

        writer.store(&e1).expect("failed to save event 1");
        writer.store(&e2).expect("failed to save event 2");
        writer.store(&e3).expect("failed to save event 3");
        writer.store(&e4).expect("failed to save event 4");

        writer.expire_events_before(time::from_millis_since_epoch(20)).expect("failed to expire events");

        let iter = reader.load_range(&VersionVector::new(), 5).expect("failed to create iter");
        let actual = iter.map(|result| result.expect("failed to read event")).collect::<Vec<_>>();
        let expected = vec![e3, e4];
        assert_eq!(expected, actual);
    }

    #[test]
    fn events_are_written_to_multiple_segments() {
        let storage_dir = TempDir::new("events_are_expired").unwrap();

        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            event_retention_duration: Duration::milliseconds(100),
            event_eviction_period: Duration::milliseconds(10)
        };

        fn event(time: u64, counter: u64) -> OwnedFloEvent {
            OwnedFloEvent::new(FloEventId::new(1, counter), None, time::from_millis_since_epoch(time), "foo".to_owned(), Vec::new())
        }

        let e1 = event(1, 1);
        let e2 = event(10, 2);
        let e3 = event(20, 3);
        let e4 = event(30, 4);

        {
            let (mut writer, _, _) = FSStorageEngine::initialize(storage_opts.clone()).expect("failed to initialize engine");

            writer.store(&e1).expect("failed to save event 1");
            writer.store(&e2).expect("failed to save event 2");
            writer.store(&e3).expect("failed to save event 3");
            writer.store(&e4).expect("failed to save event 4");
        }

        let (_, mut reader, _) = FSStorageEngine::initialize(storage_opts).expect("failed to initialize engine");

        let iter = reader.load_range(&VersionVector::new(), 5).expect("failed to create iter");
        let actual = iter.map(|result| result.expect("failed to read event")).collect::<Vec<_>>();
        let expected = vec![e1, e2, e3, e4];
        assert_eq!(expected, actual);

    }

    #[test]
    fn storage_engine_initialized_from_preexisting_events() {

        let storage_dir = TempDir::new("events_are_written_and_read_from_preexisting_directory").unwrap();
        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            event_retention_duration: Duration::days(30),
            event_eviction_period: Duration::hours(6)
        };

        let event1 = OwnedFloEvent::new(FloEventId::new(1, 1), None, event_time(), "/foo/bar".to_owned(), "first event data".as_bytes().to_owned());
        let event2 = OwnedFloEvent::new(FloEventId::new(2, 2), None, event_time(), "/nacho/cheese".to_owned(), "second event data".as_bytes().to_owned());
        let event3 = OwnedFloEvent::new(FloEventId::new(2, 3), None, event_time(), "/smalls/yourekillinme".to_owned(), "third event data".as_bytes().to_owned());


        {
            let (mut writer, _, _) = FSStorageEngine::initialize(storage_opts.clone()).unwrap();

            writer.store(&event1).expect("Failed to store event 1");
            writer.store(&event2).expect("Failed to store event 2");
            writer.store(&event3).expect("Failed to store event 3");
        }

        let (mut writer, mut reader, version_vec) = FSStorageEngine::initialize(storage_opts).expect("failed to initialize storage engine");

        let mut consumer_vector = VersionVector::new();
        consumer_vector.set(FloEventId::new(1, 1));
        let mut event_iter = reader.load_range(&consumer_vector, 55).unwrap();

        let event4 = OwnedFloEvent::new(FloEventId::new(1, 4), None, event_time(), "/yolo".to_owned(), "fourth event data".as_bytes().to_owned());
        writer.store(&event4).unwrap();

        let result = event_iter.next().expect("expected result to be Some").expect("failed to read event 3");
        assert_eq!(event2, result);

        let result = event_iter.next().expect("expected result to be Some").expect("failed to read event 4");
        assert_eq!(event3, result);

        assert!(event_iter.next().is_none()); // Assert that event4 is NOT observed

        // version vec still has counter of 1 from when version vec was initialized
        assert_eq!(1, version_vec.get(1));
        assert_eq!(3, version_vec.get(2));
    }


    #[test]
    fn events_are_stored_and_read_starting_in_the_middle_with_fresh_directory() {
        let event1 = OwnedFloEvent::new(FloEventId::new(1, 1), None, event_time(), "/foo/bar".to_owned(), "first event data".as_bytes().to_owned());
        let event2 = OwnedFloEvent::new(FloEventId::new(1, 2), None, event_time(), "/nacho/cheese".to_owned(), "second event data".as_bytes().to_owned());
        let event3 = OwnedFloEvent::new(FloEventId::new(1, 3), None, event_time(), "/smalls/yourekillinme".to_owned(), "third event data".as_bytes().to_owned());

        let storage_dir = TempDir::new("events_are_stored_and_read_starting_in_the_middle_with_fresh_directory").unwrap();
        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            event_retention_duration: Duration::days(30),
            event_eviction_period: Duration::hours(6)
        };
        let (mut writer, mut reader, _) = FSStorageEngine::initialize(storage_opts).unwrap();

        writer.store(&event1).expect("Failed to store event 1");
        writer.store(&event2).expect("Failed to store event 2");
        writer.store(&event3).expect("Failed to store event 3");

        let mut version_vector = VersionVector::new();
        version_vector.set(FloEventId::new(1, 1));
        let mut iter = reader.load_range(&version_vector, 1).unwrap();
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
        let index = Arc::new(RwLock::new(EventIndex::new()));
        let storage_opts = StorageEngineOptions {
            storage_dir: storage_dir.path().to_owned(),
            root_namespace: "default".to_owned(),
            event_retention_duration: Duration::days(30),
            event_eviction_period: Duration::hours(6)
        };
        let (mut writer, mut reader, _) = FSStorageEngine::initialize(storage_opts.clone()).expect("Failed to initialize storage engine");

        writer.store(&event1).expect("Failed to store event 1");
        writer.store(&event2).expect("Failed to store event 2");
        writer.store(&event3).expect("Failed to store event 3");

        let mut iter = reader.load_range(&VersionVector::new(), 999999).expect("failed to initialize iterator");
        let result = iter.next().unwrap().expect("Expected event1, got error");
        assert_eq!(event1, result);

        let result = iter.next().unwrap().expect("Expected event2, got error");
        assert_eq!(event2, result);

        let result = iter.next().unwrap().expect("Expected event3, got error");
        assert_eq!(event3, result);

        assert!(iter.next().is_none());
    }

}
