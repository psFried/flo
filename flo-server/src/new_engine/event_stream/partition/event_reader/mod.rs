mod namespace;

use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::collections::VecDeque;
use std::io;

use event::{FloEvent, ActorId, EventCounter};

use new_engine::ConnectionId;
use new_engine::event_stream::partition::SharedReaderRefs;
use new_engine::event_stream::partition::segment::{SegmentReader, PersistentEvent};

pub use self::namespace::NamespaceGlob;

#[derive(Debug, PartialEq, Clone)]
pub enum EventFilter {
    All,
    Glob(NamespaceGlob)
}

impl EventFilter {
    pub fn matches<E: FloEvent>(&self, event: &E) -> bool {
        match *self {
            EventFilter::All => true,
            EventFilter::Glob(ref glob) => glob.matches(event.namespace()),
        }
    }
}

//TODO: fill in event reader to iterate events in a partition
#[derive(Debug)]
pub struct PartitionReader {
    connection_id: ConnectionId,
    partition_num: ActorId,
    filter: EventFilter,
    current_segment_reader: SegmentReader,
    segment_readers_ref: SharedReaderRefs,
    returned_error: bool,
}


impl PartitionReader {

    pub fn new(connection_id: ConnectionId, partition_num: ActorId, filter: EventFilter, current_reader: SegmentReader, segment_refs: SharedReaderRefs) -> PartitionReader {
        PartitionReader {
            connection_id: connection_id,
            partition_num: partition_num,
            filter: filter,
            current_segment_reader: current_reader,
            segment_readers_ref: segment_refs,
            returned_error: false,
        }
    }

    pub fn read_next(&mut self) -> Option<io::Result<PersistentEvent>> {
        if self.returned_error {
            return None;
        }

        if self.current_segment_reader.is_exhausted() {
            if let Some(mut next_segment) = self.segment_readers_ref.get_next_segment(self.current_segment_reader.segment_id) {
                if next_segment.segment_id.0 - self.current_segment_reader.segment_id.0 > 1 {
                    warn!("Consumer for connection_id: {} skipped from {} to {}", self.connection_id, self.current_segment_reader.segment_id, next_segment.segment_id);
                } else {
                    debug!("Advanced segment for connection_id: {} to {}", self.connection_id, next_segment.segment_id);

                }
                ::std::mem::swap(&mut self.current_segment_reader, &mut next_segment);
            } else {
                return None;
            }
        }

        let next = self.current_segment_reader.read_next();
        if next.as_ref().map(|r| r.is_err()).unwrap_or(false) {
            self.returned_error = true;
        }
        next
    }
}

impl Iterator for PartitionReader {
    type Item = io::Result<PersistentEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}

