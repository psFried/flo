use std::fs::File;
use std::path::{PathBuf, Path};
use std::io::{self, Seek, SeekFrom};

use event::Event;

pub type ReadResult = Result<Event, io::Error>;

pub struct FileReader {
    storage_file_path: PathBuf,
}

impl FileReader {
    pub fn new(storage_path: PathBuf) -> FileReader {
        FileReader { storage_file_path: storage_path }
    }

    pub fn read_from_offset(&self, offset: u64) -> impl Iterator<Item=Event> {
        use std::fs::OpenOptions;
        use event_store::serialization::EventStreamDeserializer;

        let mut file: File = OpenOptions::new().read(true).write(false).open(&self.storage_file_path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        EventStreamDeserializer::new(file)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempdir::TempDir;
    use event::{Event, EventId};
    use std::fs::File;
    use std::path::PathBuf;
    use event_store::serialization::{EventSerializer, size_on_disk};

    #[test]
    fn file_reader_returns_events_starting_at_a_specified_offset() {
        let temp_dir = TempDir::new("file_reader_test").unwrap();
        let (file_path, events) = write_test_events(&temp_dir);

        let expected_event_id: EventId = 6;
        let offset = events.iter()
                           .take_while(|event| event.get_id() < expected_event_id)
                           .map(|event| size_on_disk(event) as u64)
                           .sum();
        let reader = FileReader::new(file_path.clone());
        let results = reader.read_from_offset(offset).collect::<Vec<Event>>();
        assert_eq!(5, results.len());
        assert_eq!(expected_event_id, results[0].get_id());
    }

    #[test]
    fn events_from_disk_iterates_over_events_in_file() {
        let temp_dir = TempDir::new("file_reader_test").unwrap();
        let (file_path, _) = write_test_events(&temp_dir);

        let reader = FileReader::new(file_path.clone());
        let results = reader.read_from_offset(0).collect::<Vec<Event>>();
        assert_eq!(10, results.len());
        for i in 0..10 {
            let actual_id = results[i].get_id();
            let expected_id = i as u64 + 1;
            assert_eq!(expected_id, actual_id);
        }
    }

    fn write_test_events(temp_dir: &TempDir) -> (PathBuf, Vec<Event>) {
        use std::io::copy;

        let file_path = temp_dir.path().to_owned().join("myEventsFile.json");
        let mut event_file = File::create(&file_path).unwrap();

        let mut events = Vec::new();
        for i in 1..11 {
            let event_data = "eventBytes".to_owned().into_bytes();
            let event = Event::new(i, event_data);

            {
                let mut serializer = EventSerializer::new(&event);
                copy(&mut serializer, &mut event_file).unwrap();
            }
            events.push(event);
        }

        (file_path, events)
    }
}
