

use std::cell::UnsafeCell;
use std::io;
use std::sync::atomic::{AtomicUsize, fence, Ordering};
use std::sync::{Arc, Mutex};
use std::fmt::{self, Debug};
use std::path::PathBuf;

use memmap::Mmap;

use engine::event_stream::partition::segment::PersistentEvent;
use engine::event_stream::partition::SegmentNum;
use engine::event_stream::partition::index::{PartitionIndex, IndexEntry};
use event::{FloEvent, EventCounter};
use super::header::SegmentHeader;



pub struct MmapInner {
    region: UnsafeCell<Mmap>,
    delete: Mutex<Option<SegmentDeleter>>,
    head: AtomicUsize,
}


impl MmapInner {
    pub fn get_read_slice(&self, offset: usize) -> &[u8] {
        let end = self.head.load(Ordering::Relaxed);
        if offset > end {
            panic!("Range out of bounds! requested offset: {}, region_len: {}", offset, end);
        }
        unsafe {
            &self.region_ref().as_slice()[offset..end]
        }
    }

    unsafe fn get_write_slice(&self, start_offset: usize) -> &mut [u8] {
        let mmap = &mut *self.region.get();
        &mut mmap.as_mut_slice()[start_offset..]
    }

    unsafe fn flush(&self) -> io::Result<()> {
        let mmap = &mut *self.region.get();
        let head = self.head.load(Ordering::Relaxed);
        let len = mmap.len() - head;
        mmap.flush_range(head, len)
    }

    fn region_ref(&self) -> &Mmap {
        unsafe { &*self.region.get() }
    }

}

pub type MmapRef = Arc<MmapInner>;

impl Debug for MmapInner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let region_ptr = self.region_ref().ptr();
        let region_end = self.head.load(Ordering::Relaxed);
        let delete = self.delete.lock().unwrap().is_some();
        f.debug_struct("MmapInner")
            .field("region_ptr", &region_ptr)
            .field("region_len", &region_end)
            .field("delete", &delete)
            .finish()
    }
}
//TODO: might not actually be needed
unsafe impl Sync for MmapInner {}

#[derive(Debug)]
struct SegmentDeleter(PathBuf);

impl Drop for SegmentDeleter {
    fn drop(&mut self) {
        info!("About to delete segment file: {:?}", self.0);
        let result = ::std::fs::remove_file(&self.0);
        if let Err(err) = result {
            error!("Error deleting segment file: {:?} - {:?}", self.0, err);
        }
    }
}

#[derive(Debug)]
pub struct MmapAppender {
    dirty: bool,
    inner: MmapRef,
    file_path: PathBuf,
    pub last_event_counter: EventCounter,
}


impl MmapAppender {
    pub fn new(mmap: Mmap, start_position: usize, file_path: PathBuf) -> MmapAppender {
        let inner = MmapInner {
            region: UnsafeCell::new(mmap),
            delete: Mutex::new(None),
            head: AtomicUsize::new(start_position),
        };
        MmapAppender {
            dirty: false,
            inner: Arc::new(inner),
            file_path,
            last_event_counter: 0,
        }
    }


    pub fn init_existing(mmap: Mmap, segment_num: SegmentNum, index: &mut PartitionIndex, file_path: PathBuf) -> MmapAppender {
        // Files are pre-allocated, so the number of bytes in the file will be more than what's actually been written to.
        // We'll first create the appender, then use a reader to figure out where the end of the file is.
        // While we're at it, we'll initialize the index as well
        let header_len = SegmentHeader::get_repr_length();
        let file_len = mmap.len();
        let mut appender = MmapAppender::new(mmap, file_len, file_path);
        let mut reader = appender.reader(header_len);

        let mut event_count = 0;
        let mut highest_counter = 0;
        while let Some(Ok(event)) = reader.next() {
            let entry = IndexEntry::new(event.id().event_counter, segment_num, event.file_offset());
            index.append(entry);
            event_count += 1;
            highest_counter = event.id().event_counter;
        }
        appender.last_event_counter = highest_counter;
        let head = reader.current_offset;
        debug!("initialized appender to existing segment with {} events and total len: {}", event_count, head);
        // reset the head to the actual value, now that we know what it is
        appender.inner.head.store(head, Ordering::SeqCst);
        appender
    }

    pub fn delete_on_drop(&mut self) {
        let path = self.file_path.clone();
        let mut delete = self.inner.delete.lock().unwrap();
        *delete = Some(SegmentDeleter(path));
    }

    pub fn append<E: FloEvent>(&mut self, event: &E) -> io::Result<Option<usize>> {
        unsafe {
            let event_len = PersistentEvent::get_repr_length(event) as usize;
            let start_offset = self.inner.head.load(Ordering::Relaxed);
            debug!("will write event: {} starting at offset: {}", event.id(), start_offset);
            let write_slice = self.inner.get_write_slice(start_offset);

            if write_slice.len() < event_len {
                return Ok(None);
            }

            PersistentEvent::write_unchecked(event, write_slice);
            self.inner.head.fetch_add(event_len, Ordering::SeqCst);
            self.dirty = true;
            self.last_event_counter = event.id().event_counter;

            Ok(Some(start_offset))
        }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.dirty {
            unsafe {
                self.inner.flush()?;
            }
            self.dirty = false;
        }
        Ok(())
    }

    pub fn get_file_position(&self) -> usize {
        self.inner.head.load(Ordering::SeqCst)
    }

    pub fn reader(&self, start_offset: usize) -> MmapReader {
        MmapReader {
            inner: self.inner.clone(),
            current_offset: start_offset,
        }
    }

}


#[derive(Clone, Debug)]
pub struct MmapReader {
    inner: MmapRef,
    current_offset: usize,
}



impl MmapReader {
    pub fn read_next(&mut self) -> Option<io::Result<PersistentEvent>> {
        // This bit synchronizes with the store to the `head` value in the Appender
        let current_head = self.inner.head.load(Ordering::Relaxed);
        fence(Ordering::Acquire);

        if self.current_offset >= current_head {
            return None;
        }

        let result = PersistentEvent::read(&self.inner, self.current_offset);
        if let Ok(event) = result.as_ref() {
            self.current_offset += event.total_repr_len();
        }

        Some(result)
    }

    pub fn set_offset(&mut self, new_offset: usize) {
        self.current_offset = new_offset;
    }

    pub fn set_offset_to_end(&mut self) {
        let max = self.get_current_head();
        self.set_offset(max);
    }

    pub fn is_exhausted(&self) -> bool {
        let head = self.get_current_head();
        self.current_offset >= head
    }

    fn get_current_head(&self) -> usize {
        self.inner.head.load(Ordering::Relaxed)
    }
}

impl Iterator for MmapReader {
    type Item = io::Result<PersistentEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use super::*;
    use memmap::{Mmap, Protection};
    use event::{OwnedFloEvent, FloEvent, FloEventId, time};

    fn anon_mmap() -> MmapAppender {
        let mmap = Mmap::anonymous(1024, Protection::ReadWrite).unwrap();
        MmapAppender::new(mmap, 0, PathBuf::new())
    }

    #[test]
    fn write_and_read_an_event_with_zero_lengh_namespace_and_data() {
        let mut subject = anon_mmap();
        let mut reader = subject.reader(0);
        let input = OwnedFloEvent::new(
            FloEventId::new(3, 4),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "".to_owned(),
            Vec::new());

        subject.append(&input).unwrap();
        let result = reader.read_next().expect("reader returned none").expect("failed to read event");
        assert_eq!(input, result.to_owned());
    }

    #[test]
    fn read_event_returns_error_when_namespace_length_is_too_large() {
        assert_read_err("namespace length too large", |buf| {
            buf[41] = 56;
        })
    }

    #[test]
    fn read_event_returns_error_when_namespace_length_is_too_small() {
        assert_read_err("mismatched lengths", |buf| {
            buf[43] = 7; //make the namespace length 7 instead of 8
        })
    }

    #[test]
    fn read_event_returns_error_when_data_length_is_too_large() {
        assert_read_err("mismatched lengths", |buf| {
            buf[55] = 6; //make the data length 6 instead of 5
        })
    }

    #[test]
    fn read_event_returns_error_when_data_length_is_too_small() {
        assert_read_err("mismatched lengths", |buf| {
            buf[55] = 4; //make the data length 4 instead of 5
        })
    }


    #[test]
    fn write_an_event_and_read_it_back() {
        let mut subject = anon_mmap();
        let mut reader = subject.reader(0);
        let input = OwnedFloEvent::new(
            FloEventId::new(3, 4),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "/foo/bar".to_owned(),
            vec![1, 2, 3, 4, 5]);

        subject.append(&input).unwrap();
        let result = reader.read_next().expect("reader returned none").expect("failed to read event");
        assert_eq!(input, result.to_owned());

        let len = PersistentEvent::get_repr_length(&input);
        assert_eq!(len, PersistentEvent::get_repr_length(&result));
    }

    #[test]
    fn write_many_events_then_read_back() {
        let mut subject = anon_mmap();
        let input1 = OwnedFloEvent::new(
            FloEventId::new(3, 4),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "/foo/bar".to_owned(),
            vec![1, 2, 3, 4, 5]);
        let input2 = OwnedFloEvent::new(
            FloEventId::new(3, 5),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "/foo/bar".to_owned(),
            vec![1, 2, 3, 4, 5]);
        let input3 = OwnedFloEvent::new(
            FloEventId::new(3, 7),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "/foo/bar".to_owned(),
            vec![1, 2, 3, 4, 5]);

        let _off_1 = subject.append(&input1).unwrap().expect("write returned none");
        let off_2 = subject.append(&input2).unwrap().expect("write returned none");
        let off_3 = subject.append(&input3).unwrap().expect("write returned none");

        let read_all = subject.reader(0)
            .map(|result| result.expect("failed to read event").to_owned())
            .collect::<Vec<OwnedFloEvent>>();
        let expected = vec![input1.clone(), input2.clone(), input3.clone()];
        assert_eq!(expected, read_all);

        let read_2 = subject.reader(off_2).next().unwrap().expect("failed to read event 2");
        let read2_offset = read_2.file_offset();
        assert_eq!(off_2, read2_offset);
        assert_eq!(input2, read_2.to_owned());

        let read_3 = subject.reader(off_3).next().unwrap().expect("failed to read event 3");
        assert_eq!(input3, read_3.to_owned());
    }

    fn assert_read_err<F: Fn(&mut [u8])>(expected_description: &str, modify_buffer_fun: F) {
        use std::error::Error;

        let mut subject = anon_mmap();
        let mut reader = subject.reader(0);

        let input = OwnedFloEvent::new(
            FloEventId::new(3, 4),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "/foo/bar".to_owned(),
            vec![1, 2, 3, 4, 5]);

        subject.append(&input).unwrap();

        unsafe {
            let buff = subject.inner.get_write_slice(0);
            modify_buffer_fun(buff);
        }

        let err_result = reader.read_next().expect("read next returned none");
        assert!(err_result.is_err());
        let io_err = err_result.unwrap_err();
        assert_eq!(expected_description, io_err.description());
    }
}
