mod namespace;

use std::io;

use event::{FloEvent, ActorId};

use engine::ConnectionId;
use engine::event_stream::partition::{SharedReaderRefs, SegmentNum};
use engine::event_stream::partition::segment::{SegmentReader, PersistentEvent};
use atomics::AtomicCounterReader;

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

    pub fn parse(string: &str) -> Result<EventFilter, String> {
        if string == "/**/*" || string == "**/*" {
            Ok(EventFilter::All)
        } else {
            NamespaceGlob::new(string).map(|glob| EventFilter::Glob(glob))
        }
    }
}

#[derive(Debug)]
pub struct PartitionReader {
    connection_id: ConnectionId,
    partition_num: ActorId,
    filter: EventFilter,
    commit_index_reader: AtomicCounterReader,
    current_segment_reader: Option<SegmentReader>,
    segment_readers_ref: SharedReaderRefs,
    returned_error: bool,
}


impl PartitionReader {

    pub fn new(connection_id: ConnectionId,
               partition_num: ActorId,
               filter: EventFilter,
               current_reader: Option<SegmentReader>,
               segment_refs: SharedReaderRefs,
               commit_index_reader: AtomicCounterReader) -> PartitionReader {

        PartitionReader {
            connection_id,
            partition_num,
            filter,
            commit_index_reader,
            current_segment_reader: current_reader,
            segment_readers_ref: segment_refs,
            returned_error: false,
        }
    }

    pub fn read_next_uncommitted(&mut self) -> Option<io::Result<PersistentEvent>> {
        let mut next = self.read_next();
        while self.should_skip(&next) {
            next = self.read_next();
        }
        next
    }

    pub fn read_next_committed(&mut self) -> Option<io::Result<PersistentEvent>> {
        unimplemented!()
    }

    pub fn set_to(&mut self, segment_num: SegmentNum, offset: usize) -> io::Result<()> {
        if !segment_num.is_set() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Cannot set PartitionReader segment to 0"));
        }
        if self.current_reader_segment_id() != segment_num.0 {
            let segment = self.segment_readers_ref.get_segment(segment_num)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, format!("No segment exists for {:?}", segment_num))
                    })?; // return early if segment does not exist
            self.current_segment_reader = Some(segment);
        }
        self.current_segment_reader.as_mut().unwrap().set_offset(offset);
        Ok(())
    }

    pub fn get_current_file_offset(&self) -> (SegmentNum, usize) {
        self.current_segment_reader.as_ref().map(|segment| {
            (segment.segment_id, segment.current_offset())
        }).unwrap_or((SegmentNum(0), 0))
    }

    fn should_skip(&self, result: &Option<Result<PersistentEvent, io::Error>>) -> bool {
        if let Some(Ok(ref event)) = *result {
            !self.filter.matches(event)
        } else {
            false
        }
    }

    fn current_reader_is_exhausted(&self) -> bool {
        self.current_segment_reader.as_ref().map(|reader| {
            reader.is_exhausted()
        }).unwrap_or(true)
    }

    fn current_reader_segment_id(&self) -> u64 {
        self.current_segment_reader.as_ref().map(|r| r.segment_id.0).unwrap_or(0)
    }

    fn read_next(&mut self) -> Option<io::Result<PersistentEvent>> {
        if self.returned_error {
            return None;
        }

        if self.current_reader_is_exhausted() {
            let current_segment_id = self.current_reader_segment_id();
            if let Some(next_segment) = self.segment_readers_ref.get_next_segment(SegmentNum(current_segment_id)) {
                if next_segment.segment_id.0 - current_segment_id > 1 {
                    warn!("Consumer for connection_id: {} skipped from {} to {}", self.connection_id, current_segment_id, next_segment.segment_id);
                } else {
                    debug!("Advanced segment for connection_id: {} to {}", self.connection_id, next_segment.segment_id);
                }
                self.current_segment_reader = Some(next_segment);
            } else {
                return None;
            }
        }

        let next = self.current_segment_reader.as_mut().and_then(|reader| {
            reader.read_next()
        });
        if next.as_ref().map(|r| r.is_err()).unwrap_or(false) {
            self.returned_error = true;
        }
        next
    }
}

impl Iterator for PartitionReader {
    type Item = io::Result<PersistentEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next_uncommitted()
    }
}

