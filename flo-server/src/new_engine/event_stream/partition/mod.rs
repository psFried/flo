mod segment;
mod index;
mod event_reader;
pub mod controller;

use std::fmt::{self, Debug, Display};
use std::time::{Instant, Duration};
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::io;

use new_engine::{ClientSender, ConnectionId};
use new_engine::event_stream::EventStreamOptions;
use protocol::{ProduceEvent};
use event::{FloEventId, EventCounter, ActorId};
use self::segment::{Segment, SegmentReader};
use self::controller::PartitionImpl;

pub use self::event_reader::{PartitionReader, EventFilter};

pub type PartitionSender = ::std::sync::mpsc::Sender<Operation>;
pub type PartitionReceiver = ::std::sync::mpsc::Receiver<Operation>;

pub fn create_partition_channels() -> (PartitionSender, PartitionReceiver) {
    ::std::sync::mpsc::channel()
}

pub enum PartitionSendError {
    OutOfBounds(Operation),
    ChannelError(Operation)
}

pub type PartitionSendResult = Result<(), PartitionSendError>;

pub const DATA_FILE_EXTENSION: &'static str = ".events";

fn get_events_file(partition_dir: &Path, segment_num: SegmentNum) -> PathBuf {
    let filename = format!("{}{}", segment_num.0, DATA_FILE_EXTENSION);
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

    pub fn next(&self) -> SegmentNum {
        SegmentNum(self.0 + 1)
    }

    pub fn previous(&self) -> SegmentNum {
        // TODO: think about this
        SegmentNum(self.0.saturating_sub(1))
    }
}

impl Display for SegmentNum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentNum({})", self.0)
    }
}

#[derive(Clone)]
pub struct SharedReaderRefsMut {
    inner: Arc<RwLock<VecDeque<SegmentReader>>>
}

impl SharedReaderRefsMut {
    pub fn new() -> SharedReaderRefsMut {
        SharedReaderRefsMut::with_capacity(4)
    }

    pub fn with_capacity(init_capacity: usize) -> SharedReaderRefsMut {
        SharedReaderRefsMut {
            inner: Arc::new(RwLock::new(VecDeque::with_capacity(init_capacity)))
        }
    }

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



#[derive(Clone, Debug)]
pub struct PartitionRef {
    event_stream_name: String,
    partition_num: ActorId,
    sender: PartitionSender,
}

impl PartitionRef {
    pub fn new(event_stream_name: String, partition_num: ActorId, sender: PartitionSender) -> PartitionRef {
        PartitionRef{
            event_stream_name: event_stream_name,
            partition_num: partition_num,
            sender: sender,
        }
    }
    pub fn partition_num(&self) -> ActorId {
        self.partition_num
    }

    pub fn event_stream_name(&self) -> &str {
        &self.event_stream_name
    }

    fn send(&mut self, op: Operation) -> PartitionSendResult {
        self.sender.send(op).map_err(|err| {
            PartitionSendError::ChannelError(err.0)
        })
    }
}



pub fn initialize_existing_partition(partition_num: ActorId, event_stream_data_dir: &Path, event_stream_options: &EventStreamOptions) -> io::Result<PartitionRef> {

    let partition_data_dir = get_partition_data_dir(event_stream_data_dir, partition_num);
    let partition_impl = PartitionImpl::init_existing(partition_num, partition_data_dir, event_stream_options)?;
    run_partition(partition_impl)
}

pub fn initialize_new_partition(partition_num: ActorId, event_stream_data_dir: &Path, event_stream_options: &EventStreamOptions) -> io::Result<PartitionRef> {

    let partition_data_dir = get_partition_data_dir(event_stream_data_dir, partition_num);
    let partition_impl = PartitionImpl::init_existing(partition_num, partition_data_dir, &event_stream_options)?;
    run_partition(partition_impl)
}

pub fn run_partition(partition_impl: PartitionImpl) -> io::Result<PartitionRef> {
    let partition_num = partition_impl.partition_num();
    let event_stream_name = partition_impl.event_stream_name().to_owned();
    let (tx, rx) = create_partition_channels();
    let thread_name = get_partition_thread_name(partition_impl.event_stream_name(), partition_num);
    let join_handle = thread::Builder::new().name(thread_name).spawn(move || {
        info!("Starting partition: {} of event stream: '{}'", &partition_impl.event_stream_name(), partition_num);

        let mut partition_controller = partition_impl;

        loop {
            if let Ok(message) = rx.recv() {
                let process_result = partition_controller.process(message);
                if let Err(io_err) = process_result {
                    error!("Error in partition: {} of event stream: '{}': {:?}", partition_num, partition_controller.event_stream_name(), io_err);
                }
            } else {
                break;
            }
        }
        let fsync_result = partition_controller.fsync();
        info!("Shutdown partition: {} of event stream: '{}' with fsync result: {:?}",
              partition_num,
              partition_controller.event_stream_name(),
              fsync_result);
    })?;

    Ok(PartitionRef::new(event_stream_name, partition_num,tx))
}

fn get_partition_thread_name(event_stream_name: &str, partition_num: ActorId) -> String {
    format!("partition_{}_{}", event_stream_name, partition_num)
}

pub fn get_partition_data_dir(event_stream_dir: &Path, partition_num: ActorId) -> PathBuf {
    event_stream_dir.join(format!("{}", partition_num))
}

pub type ProduceResult = ::std::io::Result<FloEventId>;
pub type AsyncProduceResult = ::futures::sync::oneshot::Receiver<ProduceResult>;



