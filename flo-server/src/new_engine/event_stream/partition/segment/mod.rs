mod persistent_event;

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use memmap::{Mmap, MmapViewSync, Protection};
use chrono::Duration;

use new_engine::event_stream::partition::get_events_file;
use event::{EventCounter, Timestamp, FloEvent};

pub use self::persistent_event::PersistentEvent;


pub struct Segment {
    mmap: MmapViewSync,
    segment_file: File,
    segment_num: u64,
    first_event_time: Option<Timestamp>,
    first_event_counter: Option<EventCounter>,
    current_length_bytes: usize,
    last_flush_range_end: usize,
    max_length_bytes: usize,
    max_duration: Duration,
}

impl Segment {

    pub fn initialize(directory_path: &Path, segment_num: u64, max_size: usize, max_duration: Duration) -> io::Result<Segment> {
        let file_path = get_events_file(directory_path, segment_num);
        if file_path.exists() {
            Segment::init_from_existing_file(&file_path, segment_num, max_size, max_duration)
        } else {
            Segment::init_new(&file_path, segment_num, max_size, max_duration)
        }
    }

    pub fn append<E: FloEvent>(&mut self, event: &E) -> io::Result<PersistentEvent> {
        let Segment {
            ref mut mmap,
            ref mut segment_file,
            ref mut segment_num,
            ref mut first_event_time,
            ref mut first_event_counter,
            ref mut current_length_bytes,
            ref mut last_flush_range_end,
            ref mut max_length_bytes,
            ref mut max_duration,
        } = *self;

        let event_len = PersistentEvent::get_repr_length(event);

        if *current_length_bytes + event_len as usize > *max_length_bytes {
            return Err(io::Error::new(io::ErrorKind::Other, "appending event would overflow segment"));
        }

        let write_result = unsafe {
            PersistentEvent::write(event, *current_length_bytes, mmap)
        };

        if write_result.is_ok() {
            *current_length_bytes += event_len as usize;
        }

        write_result
    }

    pub fn range_iter(&self, start_offset: usize) -> SegmentRangeIter {
        let mut mmap = unsafe { self.mmap.clone() };

        SegmentRangeIter {
            mmap: mmap,
            max_offset: self.current_length_bytes,
            current_offset: start_offset,
        }
    }

    pub fn fsync(&mut self) -> io::Result<()> {
        self.mmap.flush()
    }


    fn init_from_existing_file(file_path: &Path, segment_num: u64, max_size: usize, max_duration: Duration) -> io::Result<Segment> {
        unimplemented!()
    }

    fn init_new(file_path: &Path, segment_num: u64, max_size: usize, max_duration: Duration) -> io::Result<Segment> {
        debug!("initializing new segment: {} at path: {:?}, max_size: {}, max_duration: {:?}", segment_num, file_path, max_size, max_duration);
        let file = OpenOptions::new().read(true).write(true).create(true).open(file_path)?;
        // Pre-allocate the file, since we're going to use it for mmap, and extending the file after it's been mapped since
        // it requires ensuring there are no existing borrows of it in any other threads
        file.set_len(max_size as u64)?;

        let mut mmap = Mmap::open(&file, Protection::ReadWrite)?;

        Ok(Segment::from_parts(file, mmap, segment_num, max_size, max_duration))
    }

    fn from_parts(file: File, mut mmap: Mmap, segment_num: u64, max_size_bytes: usize, max_duration: Duration) -> Segment {
        let ptr = mmap.mut_ptr();
        Segment {
            mmap: mmap.into_view_sync(),
            segment_file: file,
            segment_num: segment_num,
            first_event_time: None,
            first_event_counter: None,
            current_length_bytes: 0,
            last_flush_range_end: 0,
            max_length_bytes: max_size_bytes,
            max_duration: max_duration,
        }
    }

}


pub struct SegmentRangeIter {
    mmap: MmapViewSync,
    max_offset: usize,
    current_offset: usize,
}

impl Iterator for SegmentRangeIter {
    type Item = io::Result<PersistentEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset < self.max_offset {
            let read_result = PersistentEvent::read(&self.mmap, self.current_offset);
            match read_result.as_ref() {
                Ok(event) => self.current_offset += PersistentEvent::get_repr_length(event) as usize,
                _ => {
                    self.max_offset = 0;
                }
            }
            Some(read_result)
        } else {
            None
        }
    }
}


#[cfg(test)]
mod test {
    use std::path::Path;

    use chrono::Duration;
    use tempdir::TempDir;

    use super::*;
    use event::*;

    #[test]
    fn write_one_event_to_segment_and_read_it_back() {
        let tmpdir = TempDir::new("write_events_to_segment").unwrap();

        let mut subject = Segment::initialize(tmpdir.path(), 1, 4096, Duration::seconds(2))
                .expect("failed to initialize segment");

        let event = event(1);
        let result = subject.append(&event).expect("failed to append event");
        assert_events_eq(&event, &result);

        let mut iter = subject.range_iter(0);
        let event_result = iter.next().expect("next returned None").expect("failed to read event");

        assert_events_eq(&event, &event_result);
        assert!(iter.next().is_none());
    }

    #[test]
    fn write_multiple_events_and_read_them_back() {
        let tmpdir = TempDir::new("write_events_to_segment").unwrap();

        let mut subject = Segment::initialize(tmpdir.path(), 1, 4096, Duration::seconds(2))
                .expect("failed to initialize segment");

        let input_events: Vec<OwnedFloEvent> = (1..11).map(|i| event(i)).collect();

        for event in input_events.iter() {
            let write_result = subject.append(event).expect("failed to write event");
            assert_events_eq(event, &write_result);
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


