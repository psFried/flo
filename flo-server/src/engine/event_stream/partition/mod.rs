pub mod controller;
mod segment;
mod index;
mod event_reader;
mod ops;

use std::net::SocketAddr;
use std::fmt::{self, Debug, Display};
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::thread;
use std::io;

use atomics::{AtomicCounterReader, AtomicBoolReader, AtomicBoolWriter};
use engine::ConnectionId;
use engine::event_stream::{EventStreamOptions, HighestCounter};
use engine::controller::SystemPartitionSender;
use protocol::{ProduceEvent};
use event::{EventCounter, ActorId};
use self::segment::SegmentReader;
use self::controller::PartitionImpl;

pub use self::ops::{OpType,
                    Operation,
                    ProduceOperation,
                    ConsumeOperation,
                    ProduceResult,
                    ProduceResponder,
                    ProduceResponseReceiver,
                    ConsumeResponseReceiver,
                    ConsumeResponder,
                    ConsumerNotifier,
};
pub use self::event_reader::{PartitionReader, EventFilter};
pub use self::segment::PersistentEvent;

pub type PartitionSender = ::std::sync::mpsc::Sender<Operation>;
pub type PartitionReceiver = ::std::sync::mpsc::Receiver<Operation>;

pub fn create_partition_channels() -> (PartitionSender, PartitionReceiver) {
    ::std::sync::mpsc::channel()
}

#[derive(Debug)]
pub struct PartitionSendError;

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

    /// Used to create SegmentNum(0) for testing purposes. Normally, creating such instances would only be possible from within the partition module
    #[cfg(test)]
    pub fn new_unset() -> SegmentNum {
        SegmentNum(0)
    }

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

#[derive(Clone)]
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
    #[cfg(test)]
    pub fn empty() -> SharedReaderRefs {
        SharedReaderRefs {
            inner: Arc::new(RwLock::new(VecDeque::new()))
        }
    }

    pub fn get_next_segment(&self, previous: SegmentNum) -> Option<SegmentReader> {
        let locked = self.inner.read().unwrap();
        locked.front().map(|r| r.segment_id).and_then(|front_segment| {
            let target_index = (previous.0 + 1).saturating_sub(front_segment.0);
            locked.get(target_index as usize).cloned()
        })
    }

    pub fn get_segment(&self, segment: SegmentNum) -> Option<SegmentReader> {
        if let Some(seg) =  self.get_next_segment(SegmentNum(segment.0.saturating_sub(1))) {
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


pub type AsyncProduceResult = Result<ProduceResponseReceiver, PartitionSendError>;
pub type AsyncConsumeResult = Result<ConsumeResponseReceiver, PartitionSendError>;

#[derive(Debug)]
pub struct PartitionRefMut {
    status_writer: AtomicBoolWriter,
    partition_ref: PartitionRef,
}

impl PartitionRefMut {
    pub fn partition_num(&self) -> ActorId {
        self.partition_ref.partition_num()
    }

    pub fn clone_ref(&self) -> PartitionRef {
        self.partition_ref.clone()
    }
}

#[derive(Debug, Clone)]
enum SenderType {
    Normal(PartitionSender),
    System(SystemPartitionSender)
}

#[derive(Clone, Debug)]
pub struct PartitionRef {
    event_stream_name: String,
    partition_num: ActorId,
    highest_event_counter: AtomicCounterReader,
    primary: AtomicBoolReader,
    primary_server_address: Arc<RwLock<Option<SocketAddr>>>,
    sender: SenderType,
}

impl PartitionRef {
    pub fn new(event_stream_name: String, partition_num: ActorId, highest_event_counter: AtomicCounterReader, primary: AtomicBoolReader, sender: PartitionSender, primary_server_address: Arc<RwLock<Option<SocketAddr>>>) -> PartitionRef {
        PartitionRef {
            event_stream_name,
            partition_num,
            highest_event_counter,
            primary,
            sender: SenderType::Normal(sender),
            primary_server_address,
        }
    }

    pub fn system(event_stream_name: String, partition_num: ActorId, highest_event_counter: AtomicCounterReader, primary: AtomicBoolReader, sender: SystemPartitionSender, primary_server_address: Arc<RwLock<Option<SocketAddr>>>) -> PartitionRef {
        PartitionRef {
            event_stream_name,
            partition_num,
            highest_event_counter,
            primary,
            sender: SenderType::System(sender),
            primary_server_address,
        }
    }

    pub fn partition_num(&self) -> ActorId {
        self.partition_num
    }

    pub fn get_highest_event_counter(&self) -> EventCounter {
        self.highest_event_counter.load_relaxed() as EventCounter
    }

    pub fn get_highest_counter_reader(&self) -> AtomicCounterReader {
        self.highest_event_counter.clone()
    }

    pub fn is_primary(&self) -> bool {
        self.primary.get_relaxed()
    }

    pub fn get_primary_server_addr(&self) -> Option<SocketAddr> {
        let value_ref = self.primary_server_address.read().unwrap();
        value_ref.clone()
    }

    pub fn event_stream_name(&self) -> &str {
        &self.event_stream_name
    }

    pub fn consume(&mut self, connection_id: ConnectionId, _op_id: u32, notifier: Box<ConsumerNotifier>, filter: EventFilter, start: EventCounter) -> AsyncConsumeResult {
        let (op, rx) = Operation::consume(connection_id, notifier, filter, start);
        self.send(op).map(|()| rx)
    }

    pub fn stop_consuming(&mut self, connection_id: ConnectionId) {
        let op = Operation::stop_consumer(connection_id);
        let _ = self.send(op);
    }

    pub fn produce(&mut self, connection_id: ConnectionId, op_id: u32, events: Vec<ProduceEvent>) -> AsyncProduceResult {
        let (op, rx) = Operation::produce(connection_id, op_id, events);
        self.send(op).map(|()| rx)
    }

    pub fn tick(&mut self) -> PartitionSendResult {
        self.send(Operation::tick())
    }

    pub fn send(&mut self, op: Operation) -> PartitionSendResult {
        match self.sender {
            SenderType::Normal(ref mut sender) => {
                sender.send(op).map_err(|_| {
                    PartitionSendError
                })
            }
            SenderType::System(ref mut sender) => {
                sender.send(op.into()).map_err(|_| {
                    PartitionSendError
                })
            }
        }

    }
}



pub fn initialize_existing_partition(partition_num: ActorId,
                                     event_stream_data_dir: &Path,
                                     event_stream_options: &EventStreamOptions,
                                     highest_counter: HighestCounter) -> io::Result<PartitionRefMut> {

    // TODO: for now we are starting every partition as primary. This will need to change once we have a raft implementation
    let status_writer = AtomicBoolWriter::with_value(true);
    let primary_addr = Arc::new(RwLock::new(None));

    let partition_data_dir = get_partition_data_dir(event_stream_data_dir, partition_num);
    let partition_impl = PartitionImpl::init_existing(partition_num, partition_data_dir, event_stream_options, status_writer.reader(), highest_counter)?;

    run_partition(partition_impl, primary_addr).map(|partition_ref| {
        PartitionRefMut {
            status_writer,
            partition_ref,
        }
    })
}

pub fn initialize_new_partition(partition_num: ActorId,
                                event_stream_data_dir: &Path,
                                event_stream_options: &EventStreamOptions,
                                highest_counter: HighestCounter) -> io::Result<PartitionRefMut> {

    // TODO: for now we are starting every partition as primary. This will need to change once we have a raft implementation
    let status_writer = AtomicBoolWriter::with_value(true);
    let primary_addr = Arc::new(RwLock::new(None));

    let partition_data_dir = get_partition_data_dir(event_stream_data_dir, partition_num);
    let partition_impl = PartitionImpl::init_new(partition_num, partition_data_dir, &event_stream_options, status_writer.reader(), highest_counter)?;
    run_partition(partition_impl, primary_addr).map(|partition_ref| {
        PartitionRefMut {
            status_writer,
            partition_ref,
        }
    })

}

pub fn run_partition(partition_impl: PartitionImpl, primary_server_addr: Arc<RwLock<Option<SocketAddr>>>) -> io::Result<PartitionRef> {
    let partition_num = partition_impl.partition_num();
    let event_counter_reader = partition_impl.event_counter_reader();
    let primary_status_reader = partition_impl.primary_status_reader();
    let event_stream_name = partition_impl.event_stream_name().to_owned();
    let (tx, rx) = create_partition_channels();
    let thread_name = get_partition_thread_name(partition_impl.event_stream_name(), partition_num);

    // drop the join handle and just let the thread go on its own
    // Failures will be detected by the channels used to communicate with the partition
    thread::Builder::new().name(thread_name).spawn(move || {
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

    let partition = PartitionRef::new(event_stream_name,
                                      partition_num,
                                      event_counter_reader,
                                      primary_status_reader,
                                      tx,
                                      primary_server_addr);
    Ok(partition)
}

fn get_partition_thread_name(event_stream_name: &str, partition_num: ActorId) -> String {
    format!("partition_{}_{}", event_stream_name, partition_num)
}

pub fn get_partition_data_dir(event_stream_dir: &Path, partition_num: ActorId) -> PathBuf {
    event_stream_dir.join(format!("{}", partition_num))
}



