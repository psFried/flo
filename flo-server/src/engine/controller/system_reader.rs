use std::io;

use event::FloEvent;
use engine::ConnectionId;
use engine::event_stream::partition::{PartitionReader, SegmentNum, SharedReaderRefs, EventFilter, PersistentEvent};
use engine::controller::system_event::SystemEvent;
use atomics::AtomicCounterReader;

/// The max number of events that will be sent with a single AppendEntries call.
/// This is currently a bit hacky. When the controller requests an AppendEntries to be sent, it does not say how many events
/// should be included. The controller just assumes that all available events will be sent, to a maximum of this value.
/// A better plan may be to have the controller simply tell the ConnectionHandler how many events to send.
pub const SYSTEM_READER_BATCH_SIZE: usize = 8;

#[derive(Debug)]
pub struct SystemStreamReader {
    inner: PartitionReader,
}

impl SystemStreamReader {
    pub fn new(connection_id: ConnectionId, shared_refs: SharedReaderRefs, commit_index_reader: AtomicCounterReader) -> SystemStreamReader {
        let part = PartitionReader::new(connection_id, 0, EventFilter::All, None, shared_refs, commit_index_reader);
        SystemStreamReader {
            inner: part,
        }
    }

    /// sets the reader to the given segment and offset if it's not already there
    pub fn set_to(&mut self, segment: SegmentNum, offset: usize) -> io::Result<()> {
        if segment.is_set() {
            self.inner.set_to(segment, offset)
        } else {
            self.inner.set_to_beginning();
            Ok(())
        }
    }

    pub fn fill_buffer(&mut self, event_buffer: &mut Vec<SystemEvent<PersistentEvent>>) -> io::Result<usize> {
        event_buffer.clear();
        while event_buffer.len() < SYSTEM_READER_BATCH_SIZE {
            let next = self.inner.read_next_uncommitted();
            if let Some(next_result) = next {
                let event = next_result?; // return if read failed
                let event_id = *event.id();
                let system_event = SystemEvent::from_event(event).map_err(|des_err| {
                    error!("Error deserializing system event: {}, err: {:?}", event_id, des_err);
                    io::Error::new(io::ErrorKind::InvalidData, des_err)
                })?;
                event_buffer.push(system_event);
            } else {
                break;
            }
        }
        Ok(event_buffer.len())
    }
}
