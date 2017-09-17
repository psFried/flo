mod persistent_event;
mod mmap;

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::fmt::{self, Debug};

use memmap::{Mmap, MmapViewSync, Protection};
use chrono::Duration;

use self::mmap::{MmapAppender};
use new_engine::event_stream::partition::{get_events_file, SegmentNum};
use event::{EventCounter, Timestamp, FloEvent};
use self::mmap::{MmapReader};

pub use self::persistent_event::PersistentEvent;


#[derive(Debug, Clone, PartialEq)]
pub enum AppendResult {
    Success(usize),
    EventTooBig,
    TimeOutOfRange,
    IoError(io::ErrorKind),
}

impl AppendResult {
    pub fn is_success(&self) -> bool {
        match *self {
            AppendResult::Success(_) => true,
            _ => false,
        }
    }
}

pub struct Segment {
    pub segment_num: SegmentNum,
    appender: MmapAppender,
    segment_file: File,
    current_length_bytes: usize,
    last_flush_range_end: usize,
    max_length_bytes: usize,
    segment_end_time: Timestamp,
}

impl Segment {

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

    pub fn get_end_time(&self) -> Timestamp {
        self.segment_end_time
    }

    pub fn range_iter(&self, start_offset: usize) -> SegmentReader {
        SegmentReader {
            segment_id: self.segment_num,
            reader: self.appender.reader(start_offset)
        }
    }

    pub fn fsync(&mut self) -> io::Result<()> {
        self.appender.flush()
    }


    pub fn init_from_existing_file(file_path: &Path, segment_num: SegmentNum, max_size: usize, end_time: Timestamp) -> io::Result<Segment> {
        unimplemented!()
    }

    pub fn init_new(dir_path: &Path, segment_num: SegmentNum, max_size: usize, end_time: Timestamp) -> io::Result<Segment> {
        let file_path = get_events_file(dir_path, segment_num);
        debug!("initializing new segment: {:?} at path: {:?}, max_size: {}, end_time: {:?}", segment_num, file_path, max_size, end_time);
        let file = OpenOptions::new().read(true).write(true).create(true).open(file_path)?;
        // Pre-allocate the file, since we're going to use it for mmap, and extending the file after it's been mapped
        // requires ensuring there are no existing borrows of it in any other threads. Far simpler just to pre-allocate the
        // maximum file size. Space is relatively cheap, anyway
        file.set_len(max_size as u64)?;

        let mut mmap = Mmap::open(&file, Protection::ReadWrite)?;

        Ok(Segment::from_parts(file, mmap, segment_num, max_size, end_time))
    }

    fn from_parts(file: File, mut mmap: Mmap, segment_num: SegmentNum, max_size_bytes: usize, end_time: Timestamp) -> Segment {
        let ptr = mmap.mut_ptr();
        Segment {
            appender: MmapAppender::new(mmap.into_view_sync()),
            segment_file: file,
            segment_num: segment_num,
            current_length_bytes: 0,
            last_flush_range_end: 0,
            max_length_bytes: max_size_bytes,
            segment_end_time: end_time,
        }
    }

}


#[derive(Clone, Debug)]
pub struct SegmentReader {
    pub segment_id: SegmentNum,
    reader: MmapReader,
}

impl SegmentReader {
    pub fn read_next(&mut self) -> Option<io::Result<PersistentEvent>> {
        self.reader.read_next()
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
}

impl Iterator for SegmentReader {
    type Item = io::Result<PersistentEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}


#[cfg(test)]
mod test {
    use std::path::Path;

    use chrono::Duration;
    use tempdir::TempDir;

    use super::*;
    use event::*;

    fn future_time(seconds_in_future: i64) -> Timestamp {
        time::now() + Duration::seconds(seconds_in_future)
    }

    #[test]
    fn write_one_event_to_segment_and_read_it_back() {
        let tmpdir = TempDir::new("write_events_to_segment").unwrap();

        let mut subject = Segment::init_new(tmpdir.path(), SegmentNum(1), 4096, future_time(2))
                .expect("failed to initialize segment");

        let event = event(1);
        let result = subject.append(&event);
        assert_eq!(AppendResult::Success(0), result);

        let mut iter = subject.range_iter(0);
        let event_result = iter.next().expect("next returned None").expect("failed to read event");

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

        let mut read_results: Vec<io::Result<PersistentEvent>> = subject.range_iter(0).collect();

        for (expected, actual) in input_events.iter().zip(read_results.iter()) {
            assert_events_eq(expected, actual.as_ref().expect("failed to read event"));
        }
        assert_eq!(10, read_results.len());
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


