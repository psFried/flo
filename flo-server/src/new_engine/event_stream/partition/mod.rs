mod segment;
mod index;
mod event_reader;
mod controller;

use std::fmt::{self, Debug, Display};
use std::time::Instant;
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use new_engine::{ClientSender, ConnectionId};
use protocol::{ProduceEvent};
use event::{FloEventId, EventCounter, ActorId};
use self::segment::{Segment, SegmentReader};

pub use self::event_reader::{PartitionReader, EventFilter};

pub type PartitionSender = ::std::sync::mpsc::Sender<Operation>;
pub type PartitionReceiver = ::std::sync::mpsc::Receiver<Operation>;

pub enum PartitionSendError {
    OutOfBounds(Operation),
    ChannelError(Operation)
}

pub type PartitionSendResult = Result<(), PartitionSendError>;


fn get_events_file(partition_dir: &Path, segment_num: SegmentNum) -> PathBuf {
    let filename = format!("{}.events", segment_num.0);
    partition_dir.join(filename)
}


/// A 1-based monotonically incrementing counter used to identify segments.
/// SegmentNums CANNOT BE 0! Since these are used all over in rather memory-sensitive areas, we sometimes
/// use 0 as a sentinal value to indicate the lack of a segment;
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentNum(u64);

impl SegmentNum {
    /// returns true if this segment is non-zero
    pub fn is_set(&self) -> bool {
        self.0 > 0
    }
}

impl Display for SegmentNum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentNum(")
    }
}

#[derive(Clone)]
pub struct SharedReaderRefsMut {
    inner: Arc<RwLock<VecDeque<SegmentReader>>>
}

impl SharedReaderRefsMut {
    pub fn add(&self, reader: SegmentReader) {
        let mut locked = self.inner.write().unwrap();
        locked.push_back(reader);
    }

    pub fn remove_through(&self, segment: SegmentNum) {
        let mut locked = self.inner.write().unwrap();
        while locked.front().map(|r| r.segment_id <= segment).unwrap_or(false) {
            let removed = locked.pop_front().unwrap();
            debug!("removing: {} from shared reader refs", removed.segment_id);
        }
    }

    pub fn get_reader_refs(&self) -> SharedReaderRefs {
        SharedReaderRefs {
            inner: self.inner.clone()
        }
    }
}

pub struct SharedReaderRefs {
    inner: Arc<RwLock<VecDeque<SegmentReader>>>
}

impl Debug for SharedReaderRefs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SharedReaderRefs(").and_then(|()| {
            ::std::fmt::Pointer::fmt(&self.inner, f).and_then(|()| {
                write!(f, ")")
            })
        })
    }
}

impl SharedReaderRefs {
    pub fn get_next_segment(&self, previous: SegmentNum) -> Option<SegmentReader> {
        let locked = self.inner.read().unwrap();
        locked.front().map(|r| r.segment_id).and_then(|front_segment| {
            let target_index = (previous.0 + 1).saturating_sub(front_segment.0);
            locked.get(target_index as usize).cloned()
        })
    }

    pub fn get_segment(&self, segment: SegmentNum) -> Option<SegmentReader> {
        if let Some(seg) =  self.get_next_segment(SegmentNum(segment.0 - 1)) {
            if seg.segment_id == segment {
                Some(seg)
            } else {
                None
            }
        } else {
            None
        }
    }
}



pub struct ProduceOperation {
    client: ClientSender,
    op_id: u64,
    events: Vec<ProduceEvent>,
}

impl Debug for ProduceOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ProduceOperation {{ op_id: {}, events: {:?} }}", self.op_id, self.events)
    }
}

pub struct ConsumeOperation {
    pub client_sender: ::futures::sync::oneshot::Sender<PartitionReader>,
    pub filter: EventFilter,
    pub start_exclusive: EventCounter,
}

impl Debug for ConsumeOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConsumeOperation {{ filter: {:?}, start_exclusive: {} }}", self.filter, self.start_exclusive)
    }
}

#[derive(Debug)]
pub enum OpType {
    Produce(ProduceOperation),
    Consume(ConsumeOperation),
}


#[derive(Debug)]
pub struct Operation {
    connection_id: ConnectionId,
    client_message_recv_time: Instant,
    op_type: OpType,
}



#[derive(Clone)]
pub struct PartitionRef {
    event_stream_name: String,
    partion_num: ActorId,
    sender: PartitionSender,
}

impl PartitionRef {
    fn send(&mut self, op: Operation) -> PartitionSendResult {
        self.sender.send(op).map_err(|err| {
            PartitionSendError::ChannelError(err.0)
        })
    }
}


pub type ProduceResult = ::std::io::Result<FloEventId>;
pub type AsyncProduceResult = ::futures::sync::oneshot::Receiver<ProduceResult>;



