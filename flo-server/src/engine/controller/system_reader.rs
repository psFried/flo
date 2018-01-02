use std::io;
use std::vec::Drain;

use engine::ConnectionId;
use engine::event_stream::partition::{PartitionReader, SegmentNum, SharedReaderRefs, EventFilter, PersistentEvent};
use atomics::AtomicCounterReader;

const SYSTEM_READER_BATCH_SIZE: usize = 8;

#[derive(Debug)]
pub struct SystemStreamReader {
    inner: PartitionReader,
    event_buffer: Vec<PersistentEvent>,
}

impl SystemStreamReader {
    pub fn new(connection_id: ConnectionId, shared_refs: SharedReaderRefs, commit_index_reader: AtomicCounterReader) -> SystemStreamReader {
        use engine::event_stream::partition::EventFilter;
        let part = PartitionReader::new(connection_id, 0, EventFilter::All, None, shared_refs, commit_index_reader);
        SystemStreamReader {
            inner: part,
            event_buffer: Vec::with_capacity(SYSTEM_READER_BATCH_SIZE),
        }
    }

    /// sets the reader to the given segment and offset if it's not already there
    pub fn set_to(&mut self, segment: SegmentNum, offset: usize) -> io::Result<()> {
        self.inner.set_to(segment, offset)
    }

    pub fn fill_buffer(&mut self) -> io::Result<usize> {
        while self.event_buffer.len() < SYSTEM_READER_BATCH_SIZE {
            let next = self.inner.next();
            if let Some(next_result) = next {
                let event = next_result?; // return if read failed
                self.event_buffer.push(event);
            } else {
                break;
            }
        }
        Ok(self.event_buffer.len())
    }

    pub fn drain(&mut self) -> Drain<PersistentEvent> {
        self.event_buffer.drain(..)
    }
}
