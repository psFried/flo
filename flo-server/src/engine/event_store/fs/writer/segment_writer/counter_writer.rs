
use std::io::{self, Write, Seek, BufWriter};
use std::fs::{File, OpenOptions};
use std::path::Path;

use event::{EventCounter, ActorId};
use engine::event_store::fs::{read_u64, write_u64, get_event_counter_file};

#[derive(Debug)]
pub struct EventCounterWriter {
    pub current_counter: EventCounter,
    file: File,
    _actor_id: ActorId,
    _segment_number: u64, // here just for debugging purposes
}

impl EventCounterWriter {
    pub fn initialize(partition_dir: &Path, segment_number: u64, actor_id: ActorId) -> io::Result<EventCounterWriter> {
        let counter_file_path = get_event_counter_file(partition_dir, actor_id);
        let mut counter_file = OpenOptions::new().write(true).read(true).create(true).open(&counter_file_path)?;

        let current_counter = match read_u64(&mut counter_file) {
            Ok(n) => n,
            Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => 0,
            Err(other_err) => return Err(other_err) // early return if reading file fails
        };

        Ok(EventCounterWriter {
            file: counter_file,
            _actor_id: actor_id,
            _segment_number: segment_number,
            current_counter: current_counter,
        })
    }

    pub fn commit(&mut self, new_counter: EventCounter) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(0))?;
        write_u64(new_counter, &mut self.file)?;

        self.file.flush()?;
        self.current_counter = new_counter;
        Ok(())
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use tempdir::TempDir;


    #[test]
    fn initializes_from_an_empty_directory_with_current_counter_of_0() {
        let actor = 4;
        let segment = 5;
        let temp_dir = TempDir::new("counter_writer_initializes_with_0").unwrap();

        let result = EventCounterWriter::initialize(temp_dir.path(), segment, actor).expect("failed to initialize event counter writer");
        assert_eq!(0, result.current_counter);
    }

    #[test]
    fn commit_sets_current_counter() {
        let actor = 4;
        let segment = 5;
        let temp_dir = TempDir::new("commit_sets_current_counter").unwrap();

        let mut subject = EventCounterWriter::initialize(temp_dir.path(), segment, actor).expect("failed to initialize event counter writer");
        subject.commit(8765).expect("failed to commit counter");
        assert_eq!(8765, subject.current_counter);

        subject.commit(5678).expect("failed to commit counter");
        assert_eq!(5678, subject.current_counter);

        subject.commit(999999).expect("failed to commit counter");
        assert_eq!(999999, subject.current_counter);
    }

    #[test]
    fn initializes_from_an_existing_file_with_current_id_set_to_last_committed_id() {
        let actor = 3;
        let segment = 5;
        let temp_dir = TempDir::new("initialize_from_existing").unwrap();

        {
            let mut subject = EventCounterWriter::initialize(temp_dir.path(), segment, actor).expect("failed to initialize event counter writer");
            subject.commit(8765).expect("failed to commit counter");
            subject.commit(5678).expect("failed to commit counter");
            subject.commit(12345).expect("failed to commit counter");
        }

        let subject = EventCounterWriter::initialize(temp_dir.path(), segment, actor).expect("failed to initialize event counter writer");
        assert_eq!(12345, subject.current_counter);
    }

}

