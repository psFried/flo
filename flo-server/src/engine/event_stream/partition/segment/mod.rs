mod persistent_event;
mod mmap;
mod header;

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use memmap::{Mmap, Protection};

use self::mmap::{MmapAppender};
use engine::event_stream::partition::{get_events_file, SegmentNum};
use engine::event_stream::partition::index::PartitionIndex;
use event::{Timestamp, FloEvent, EventCounter, time};
use self::mmap::{MmapReader};

pub use self::persistent_event::PersistentEvent;
use self::header::SegmentHeader;


#[derive(Debug, Clone, PartialEq)]
pub enum AppendResult {
    Success(usize),
    EventTooBig,
    TimeOutOfRange,
    IoError(io::ErrorKind),
}

impl AppendResult {
    #[cfg(test)]
    pub fn is_success(&self) -> bool {
        match *self {
            AppendResult::Success(_) => true,
            _ => false,
        }
    }
}

#[allow(dead_code)]
pub struct Segment {
    pub segment_num: SegmentNum,
    appender: MmapAppender,
    segment_file: File,
    current_length_bytes: usize,
    last_flush_range_end: usize, // TODO: implement flush to disk for durable writes
    max_length_bytes: usize, // TODO: start a new segment after max length in bytes is reached
    segment_end_time: Timestamp,
}

impl Segment {

    pub fn head_position(&self) -> usize {
        self.appender.get_file_position()
    }

    pub fn is_expired(&self, now: Timestamp) -> bool {
        now < self.segment_end_time
    }

    pub fn delete_on_drop(&mut self) {
        info!("Segment: {:?} will delete on drop", self.segment_num);
        self.appender.delete_on_drop();
    }

    pub fn get_highest_event_counter(&self) -> EventCounter {
        self.appender.last_event_counter
    }

    pub fn append<E: FloEvent>(&mut self, event: &E) -> AppendResult {
        if event.timestamp() > self.segment_end_time {
            return AppendResult::TimeOutOfRange;
        }
        match self.appender.append(event) {
            Ok(Some(offset)) => AppendResult::Success(offset),
            Ok(None) => AppendResult::EventTooBig,
            Err(io_err) => AppendResult::IoError(io_err.kind()),
        }
    }

    #[allow(dead_code)] // TODO: delete partitions after they expire
    pub fn get_end_time(&self) -> Timestamp {
        self.segment_end_time
    }

    pub fn range_iter(&self, start_offset: usize) -> SegmentReader {
        let start = ::std::cmp::max(start_offset, SegmentHeader::get_repr_length());
        trace!("creating range iter starting at offset: {}", start);
        SegmentReader {
            segment_id: self.segment_num,
            last_read_id: 0,
            reader: self.appender.reader(start)
        }
    }

    pub fn iter_from_start(&self) -> SegmentReader {
        self.range_iter(SegmentHeader::get_repr_length())
    }

    pub fn fsync(&mut self) -> io::Result<()> {
        self.appender.flush()
    }

    pub fn init_from_existing_file(file_path: &Path, segment_num: SegmentNum, index: &mut PartitionIndex) -> io::Result<Segment> {
        let file = OpenOptions::new().read(true).write(true).open(&file_path)?;
        let file_len = file.metadata()?.len() as usize;
        let mmap = Mmap::open(&file, Protection::ReadWrite)?;
        let header = SegmentHeader::read(&mmap)?;

        let mmap_appender = MmapAppender::init_existing(mmap, segment_num, index, file_path.to_owned());
        let current_position = mmap_appender.get_file_position();

        let segment = Segment {
            appender: mmap_appender,
            segment_file: file,
            segment_num: segment_num,
            current_length_bytes: current_position,
            last_flush_range_end: current_position,
            max_length_bytes: file_len,
            segment_end_time: header.end_time,
        };

        Ok(segment)
    }

    pub fn init_new(dir_path: &Path, segment_num: SegmentNum, max_size: usize, end_time: Timestamp) -> io::Result<Segment> {
        let file_path = get_events_file(dir_path, segment_num);
        debug!("initializing new segment: {:?} at path: {:?}, max_size: {}, end_time: {:?}", segment_num, file_path, max_size, end_time);
        let file = OpenOptions::new().read(true).write(true).create(true).open(&file_path)?;

        // Pre-allocate the file, since we're going to use it for mmap, and extending the file after it's been mapped
        // requires ensuring there are no existing borrows of it in any other threads. Far simpler just to pre-allocate the
        // maximum file size. Space is relatively cheap, anyway
        file.set_len(max_size as u64)?;

        let mut mmap = Mmap::open(&file, Protection::ReadWrite)?;
        let header = SegmentHeader {
            create_time: time::now(),
            end_time: end_time,
        };
        header.write(&mut mmap)?;

        let start_position = SegmentHeader::get_repr_length();

        Ok(Segment {
            appender: MmapAppender::new(mmap, start_position, file_path),
            segment_file: file,
            segment_num: segment_num,
            current_length_bytes: start_position,
            last_flush_range_end: 0,
            max_length_bytes: max_size,
            segment_end_time: end_time,
        })
    }

}


#[derive(Clone, Debug)]
pub struct SegmentReader {
    pub segment_id: SegmentNum,
    last_read_id: EventCounter,
    reader: MmapReader,
}

impl SegmentReader {
    pub fn read_next(&mut self) -> Option<io::Result<PersistentEvent>> {
        let result = self.reader.read_next();
        if let Some(&Ok(ref event)) = result.as_ref() {
            self.last_read_id = event.id().event_counter;
        }
        result
    }

    pub fn is_exhausted(&self) -> bool {
        self.reader.is_exhausted()
    }

    pub fn set_offset_to_end(&mut self) {
        self.reader.set_offset_to_end();
    }

    pub fn set_offset(&mut self, new_offset: usize) {
        self.reader.set_offset(new_offset)
    }

    pub fn current_offset(&self) -> usize {
        self.reader.get_current_offset()
    }
}

impl Iterator for SegmentReader {
    type Item = io::Result<PersistentEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}


#[cfg(test)]
mod test {
    use chrono::Duration;
    use tempdir::TempDir;

    use super::*;
    use event::*;
    use engine::event_stream::partition::index::PartitionIndex;

    fn future_time(seconds_in_future: i64) -> Timestamp {
        time::now() + Duration::seconds(seconds_in_future)
    }

    #[test]
    fn write_one_event_to_segment_and_read_it_back() {
        let tmpdir = TempDir::new("write_events_to_segment").unwrap();
        let event = event(1);
        let segment_num = SegmentNum(1);

        {
            let mut subject = Segment::init_new(tmpdir.path(), segment_num, 4096, future_time(2))
                    .expect("failed to initialize segment");

            let result = subject.append(&event);
            assert_eq!(AppendResult::Success(SegmentHeader::get_repr_length()), result);

            let mut iter = subject.iter_from_start();

            let event_result = iter.next().expect("next returned None").expect("failed to read event");

            assert_events_eq(&event, &event_result);
            assert!(iter.next().is_none());
        }

        let mut index = PartitionIndex::new(1);

        let segment_file = tmpdir.path().join("1.events");
        let subject = Segment::init_from_existing_file(&segment_file, segment_num, &mut index)
                .expect("failed to init segment from existing file");

        let mut iter = subject.iter_from_start();
        let event_result = iter.next().expect("next returned none").expect("failed to read event");
        assert_events_eq(&event, &event_result);
        assert!(iter.next().is_none());
    }

    #[test]
    fn write_multiple_events_and_read_them_back() {
        let tmpdir = TempDir::new("write_events_to_segment").unwrap();

        let mut subject = Segment::init_new(tmpdir.path(), SegmentNum(1), 4096, future_time(2))
                .expect("failed to initialize segment");

        let input_events: Vec<OwnedFloEvent> = (1..11).map(|i| event(i)).collect();

        for event in input_events.iter() {
            let write_result = subject.append(event);
            assert!(write_result.is_success());
        }

        let read_results: Vec<io::Result<PersistentEvent>> = subject.iter_from_start().collect();

        for (expected, actual) in input_events.iter().zip(read_results.iter()) {
            assert_events_eq(expected, actual.as_ref().expect("failed to read event"));
        }
        assert_eq!(10, read_results.len());
    }

    #[test]
    fn read_after_write() {
        let tmpdir = TempDir::new("read_after_write").unwrap();
        let mut subject = Segment::init_new(tmpdir.path(), SegmentNum(1), 4096, future_time(2))
                .expect("failed to initialize segment");

        let mut reader = subject.iter_from_start();
        assert!(reader.next().is_none());

        let event = event(1);

        let append_result = subject.append(&event);
        assert!(append_result.is_success());

        // make sure we can read the new event
        reader.next().expect("next returned none").expect("next returned error");
    }

    fn assert_events_eq<L: FloEvent, R: FloEvent>(lhs: &L, rhs: &R) {
        assert_eq!(lhs.id(), rhs.id());
        assert_eq!(lhs.parent_id(), rhs.parent_id());
        assert_eq!(time::millis_since_epoch(lhs.timestamp()), time::millis_since_epoch(rhs.timestamp()));
        assert_eq!(lhs.namespace(), rhs.namespace());
        assert_eq!(lhs.data_len(), rhs.data_len());
        assert_eq!(lhs.data(), rhs.data());
    }

    fn event(counter: EventCounter) -> OwnedFloEvent {
        OwnedFloEvent::new(
            FloEventId::new(1, counter),
            None,
            time::now(),
            "/foo/bar".to_owned(),
            vec![1, 2, 3, 4, 5]
        )
    }
}


