
use std::io;
use std::sync::atomic::{AtomicUsize, fence, Ordering};
use std::sync::Arc;
use std::fmt::{self, Debug};

use memmap::MmapViewSync;

use new_engine::event_stream::partition::segment::PersistentEvent;
use new_engine::event_stream::partition::SegmentNum;
use new_engine::event_stream::partition::index::{PartitionIndex, IndexEntry};
use event::FloEvent;
use super::header::SegmentHeader;


pub struct MmapAppender {
    dirty: bool,
    inner: MmapViewSync,
    head: Arc<AtomicUsize>,
}


impl MmapAppender {
    pub fn new(mmap: MmapViewSync, start_position: usize) -> MmapAppender {
        MmapAppender {
            dirty: false,
            inner: mmap,
            head: Arc::new(AtomicUsize::new(start_position)),
        }
    }

    pub fn init_existing(mmap: MmapViewSync, segment_num: SegmentNum, index: &mut PartitionIndex) -> MmapAppender {
        // Files are pre-allocated, so the number of bytes in the file will be more than what's actually been written to.
        // We'll first create the appender, then use a reader to figure out where the end of the file is.
        // While we're at it, we'll initialize the index as well
        let header_len = SegmentHeader::get_repr_length();
        let file_len = mmap.len();
        let appender = MmapAppender {
            dirty: false,
            inner: mmap,
            // temporarily set the head to be the same as the file length
            head: Arc::new(AtomicUsize::new(file_len))
        };
        let mut reader = appender.reader(header_len);

        let mut event_count = 0;
        while let Some(Ok(event)) = reader.next() {
            let entry = IndexEntry::new(event.id().event_counter, segment_num, event.file_offset());
            index.append(entry);
            event_count += 1;
        }
        let head = reader.current_offset;
        debug!("initialized appender to existing segment with {} events and total len: {}", event_count, head);
        // reset the head to the actual value, now that we know what it is
        appender.head.store(head, Ordering::SeqCst);
        appender
    }

    pub fn append<E: FloEvent>(&mut self, event: &E) -> io::Result<Option<usize>> {
        // relaxed load _should_ be ok here, right?
        let current_head = self.head.load(Ordering::Relaxed);
        //TODO: not sure if we need another fence here to make sure that the load doesn't get reordered?

        let available_space = self.inner.len() - current_head;
        let event_len = PersistentEvent::get_repr_length(event) as usize;

        if event_len > available_space {
            // This is not going to fit, so we return None to indicate that there's no room
            return Ok(None);
        }
        unsafe {
            // early return if write fails
            PersistentEvent::write(event, current_head, &mut self.inner)?;
        }
        self.dirty = true;

        // Synchronizes with the loading of the head value in the Reader
        fence(Ordering::Release);
        let offset = self.head.fetch_add(event_len, Ordering::Relaxed);
        assert_eq!(offset, current_head, "Underlying mmap region has been modified by another thread. This is a bug");

        Ok(Some(offset))
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.dirty {
            self.inner.flush()?;
            self.dirty = false;
        }
        Ok(())
    }

    pub fn get_file_position(&self) -> usize {
        self.head.load(Ordering::SeqCst)
    }

    pub fn reader(&self, start_offset: usize) -> MmapReader {
        let view = unsafe {
            self.inner.clone()
        };
        MmapReader {
            whole_segment_region: view,
            segment_end_ref: self.head.clone(),
            current_offset: start_offset,
        }
    }

}


pub struct MmapReader {
    whole_segment_region: MmapViewSync,
    segment_end_ref: Arc<AtomicUsize>,
    current_offset: usize,
}

impl ::std::clone::Clone for MmapReader {
    fn clone(&self) -> Self {
        let view = unsafe {
            self.whole_segment_region.clone()
        };

        MmapReader {
            whole_segment_region: view,
            segment_end_ref: self.segment_end_ref.clone(),
            current_offset: self.current_offset
        }
    }
}

impl Debug for MmapReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let region_ptr = self.whole_segment_region.ptr();
        let len = self.whole_segment_region.len();
        let end = self.segment_end_ref.load(Ordering::Relaxed);
        write!(f, "MmapReader {{ region_ptr: {:p}, region_len: {}, segment_end: {}, current_offset: {} }}", region_ptr, len, end, self.current_offset)
    }
}


impl MmapReader {
    pub fn read_next(&mut self) -> Option<io::Result<PersistentEvent>> {
        // This bit synchronizes with the store to the `head` value in the Appender
        let current_head = self.segment_end_ref.load(Ordering::Relaxed);
        fence(Ordering::Acquire);

        if self.current_offset >= current_head {
            return None;
        }

        let result = PersistentEvent::read(&self.whole_segment_region, self.current_offset);
        if let Ok(event) = result.as_ref() {
            self.current_offset += event.total_repr_len();
        }

        Some(result)
    }

    pub fn set_offset(&mut self, new_offset: usize) {
        self.current_offset = new_offset;
    }

    pub fn set_offset_to_end(&mut self) {
        let max = self.segment_end_ref.load(Ordering::Relaxed);
        self.set_offset(max);
    }

    pub fn is_exhausted(&self) -> bool {
        let end = self.segment_end_ref.load(Ordering::Relaxed);
        self.current_offset >= end
    }
}

impl Iterator for MmapReader {
    type Item = io::Result<PersistentEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}
