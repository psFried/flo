mod util;
mod consumer_manager;
mod commit_manager;

use std::io;
use std::collections::VecDeque;
use std::path::PathBuf;

use chrono::{Duration};

use atomics::{AtomicCounterWriter, AtomicCounterReader, AtomicBoolReader};
use protocol::ProduceEvent;
use event::{ActorId, FloEventId, EventCounter, FloEvent, Timestamp, time};
use super::{SharedReaderRefsMut, Operation, OpType, ProduceOperation, ConsumeOperation, PartitionReader, EventFilter, SegmentNum};
use super::segment::Segment;
use super::index::{PartitionIndex, IndexEntry};
use engine::event_stream::{EventStreamOptions, HighestCounter};
use engine::ConnectionId;
use self::util::get_segment_files;
use self::consumer_manager::ConsumerManager;

const FIRST_SEGMENT_NUM: SegmentNum = SegmentNum(1);

pub struct PartitionImpl {
    /// The name of the event stream that this partition is a member of. This is just here to make debugging _way_ easier.
    event_stream_name: String,
    /// The partition number within this event stream
    partition_num: ActorId,
    /// The directory used to store everything for this partition
    partition_dir: PathBuf,
    /// The maximum size in bytes for any segment. This value may be exceeded when the size of a single event is larger than
    /// the `max_segment_size`. In this case, you'll end up with a segment that includes just that one event
    max_segment_size: usize,
    /// the maximum duration of any segment. Helps control the size of segments when there's relatively low frequency of events
    /// added and a short TTL for events
    max_segment_duration: Duration,
    /// The segments that make up this partition
    segments: VecDeque<Segment>,
    /// A simple index that maps `EventCounter`s to a tuple of segment number and file offset
    index: PartitionIndex,
    /// Shared EventCounter for all partitions in the event stream. Serves as a Lamport clock to help reason about relative
    /// order of events across multiple partitions. Used to generate new `EventCounter`s when events are appended
    event_stream_highest_counter: HighestCounter,
    /// Tracks the highest committed event in this partition. This value is shared with the `ConnectionHandler`s
    partition_highest_committed: AtomicCounterWriter,
    /// Whether this instance is the primary for this partition. This value is set by `FloController`, since it requires
    /// consensus to modify which instance is primary for a partition.
    primary: AtomicBoolReader,

    /// new segments each have a reader added here. The readers are then accessed as needed by the EventReader
    reader_refs: SharedReaderRefsMut,

    /// consumers each have a notifier added here
    consumer_manager: ConsumerManager,
}

impl PartitionImpl {

    pub fn init_existing(partition_num: ActorId,
                         partition_data_dir: PathBuf,
                         options: &EventStreamOptions,
                         status_reader: AtomicBoolReader,
                         highest_counter: HighestCounter) -> io::Result<PartitionImpl> {

        let start_time = ::std::time::Instant::now();
        debug!("Starting to init partition: {} with directory: {:?}, and options: {:?}", partition_num, partition_data_dir, options);

        let mut index = PartitionIndex::new(partition_num);

        let segment_files = get_segment_files(&partition_data_dir)?;
        let mut initialized_segments = VecDeque::with_capacity(segment_files.len());
        let reader_refs = SharedReaderRefsMut::with_capacity(segment_files.len());
        for segment_file in segment_files {
            let segment = segment_file.init_segment(&mut index)?;
            let reader = segment.iter_from_start();
            initialized_segments.push_front(segment);
            reader_refs.add(reader);
        }

        //TODO: differentiate between highest committed and highest uncommitted when initializing existing partition
        let current_greatest_id = index.greatest_event_counter();
        highest_counter.set_if_greater(current_greatest_id);
        let partition_id_counter = AtomicCounterWriter::with_value(current_greatest_id as usize);

        // TODO: factor out a more legit method of timing and logging perf stats
        let init_time = start_time.elapsed();
        let time_in_millis = (init_time.as_secs() * 1000) +
            (init_time.subsec_nanos() as u64 / 1_000_000);
        info!("Initialized existing partition: {} with directory: {:?}, and options: {:?} in {} milliseconds",
              partition_num,
              partition_data_dir,
              options,
              time_in_millis);

        Ok(PartitionImpl {
            event_stream_name: options.name.clone(),
            partition_num: partition_num,
            partition_dir: partition_data_dir,
            max_segment_size: options.segment_max_size_bytes,
            max_segment_duration: options.max_segment_duration,
            segments: initialized_segments,
            index: index,
            event_stream_highest_counter: highest_counter,
            partition_highest_committed: partition_id_counter,
            primary: status_reader,
            reader_refs: reader_refs,
            consumer_manager: ConsumerManager::new(),
        })
    }

    pub fn init_new(partition_num: ActorId,
                    partition_data_dir: PathBuf,
                    options: &EventStreamOptions,
                    status_reader: AtomicBoolReader,
                    highest_counter: HighestCounter) -> io::Result<PartitionImpl> {

        ::std::fs::create_dir_all(&partition_data_dir)?;

        Ok(PartitionImpl {
            event_stream_name: options.name.to_owned(),
            partition_num: partition_num,
            partition_dir: partition_data_dir,
            max_segment_duration: options.max_segment_duration,
            max_segment_size: options.segment_max_size_bytes,
            segments: VecDeque::with_capacity(4),
            index: PartitionIndex::new(partition_num),
            event_stream_highest_counter: highest_counter,
            partition_highest_committed: AtomicCounterWriter::zero(),
            primary: status_reader,
            reader_refs: SharedReaderRefsMut::new(),
            consumer_manager: ConsumerManager::new(),
        })
    }

    pub fn event_stream_name(&self) -> &str {
        &self.event_stream_name
    }

    pub fn event_counter_reader(&self) -> AtomicCounterReader {
        self.partition_highest_committed.reader()
    }

    pub fn primary_status_reader(&self) -> AtomicBoolReader {
        self.primary.clone()
    }

    pub fn partition_num(&self) -> ActorId {
        self.partition_num
    }

    pub fn process(&mut self, operation: Operation) -> io::Result<()> {
        trace!("Partition: {}, got operation: {:?}", self.partition_num, operation);

        // TODO: time handling and log it
        let Operation{connection_id, op_type, ..} = operation;

        match op_type {
            OpType::Produce(produce_op) => {
                self.handle_produce(produce_op)
            }
            OpType::Consume(consume_op) => {
                self.handle_consume(connection_id, consume_op)
            }
            OpType::StopConsumer => {
                self.consumer_manager.remove(connection_id);
                Ok(())
            }
            OpType::Tick => {
                self.expire_old_events();
                Ok(())
            }
            other @ _ => {
                warn!("received unexpected OpType: {:?}", other);
                Ok(())
            }
        }
    }

    fn expire_old_events(&mut self) {
        let now = time::now();
        let expired_segment_index = self.segments.iter().enumerate().take_while(|&(_, ref segment)| {
            segment.is_expired(now)
        }).last().map(|(ref index, _)| *index);
        if let Some(drop_through_index) = expired_segment_index {
            self.drop_segments_through_index(drop_through_index);
        }
    }

    fn drop_segments_through_index(&mut self, segment_index: usize) {
        info!("Dropping first {} segment(s)", segment_index + 1);
        let PartitionImpl { ref mut segments, ref mut index, ref mut reader_refs, .. } = *self;

        segments.drain(..(segment_index + 1)).for_each(|mut drop_segment| {
            info!("Removing Segment: {:?} with highest_event counter: {}", drop_segment.segment_num, drop_segment.get_highest_event_counter());
            reader_refs.remove_through(drop_segment.segment_num);
            index.remove_through(drop_segment.get_highest_event_counter());
            drop_segment.delete_on_drop();
        });
    }

    fn handle_produce(&mut self, produce: ProduceOperation) -> io::Result<()> {
        let ProduceOperation {client, op_id, events} = produce;
        let result = self.append_all(events);
        if let Err(e) = result.as_ref() {
            error!("Failed to handle produce operation for op_id: {}, err: {:?}", op_id, e);
        }
        // No biggie if the receiving end has hung up already. The operation will still be considered complete and successful
        // TODO: Consider logging this if the receiving end has hung up already?
        let _ = client.send(result);
        Ok(())
    }

    fn append_all(&mut self, events: Vec<ProduceEvent>) -> io::Result<FloEventId> {
        let event_count = events.len();
        // reserve the range of ids for the events
        let new_highest = self.event_stream_highest_counter.increment_and_get(event_count as u64);

        let timestamp = time::now();
        let mut event_counter = new_highest - event_count as u64;
        for produce_event in events {
            event_counter += 1;
            let event = EventToProduce {
                id: FloEventId::new(self.partition_num, event_counter),
                ts: timestamp,
                produce: produce_event,
            };
            // early return if creating segment fails or if appending fails
            self.append(&event)?;
        }
        debug!("partition: {} finished appending {} events ending with counter: {}", self.partition_num, event_count, event_counter);

        // fence to make sure that events are actually done being saved prior to the notify, since
        // consumers may then immediately read that region of memory
        ::std::sync::atomic::fence(::std::sync::atomic::Ordering::SeqCst);
        self.consumer_manager.notify_uncommitted();
        Ok(FloEventId::new(self.partition_num, event_counter))
    }

    fn append(&mut self, event: &EventToProduce) -> io::Result<()> {
        use super::SegmentNum;
        use super::segment::AppendResult;

        let mut byte_offset: usize = 0;
        let mut segment_num: SegmentNum = SegmentNum(0);

        if let Some(ref mut segment) = self.segments.front_mut() {
            match segment.append(event) {
                AppendResult::Success(offset) => {
                    byte_offset = offset;
                    segment_num = segment.segment_num;
                }
                AppendResult::IoError(kind) => {
                    return Err(kind.into());
                }
                other @ _ => {
                    debug!("Event {} does not fit into {:?} due to: {:?}", event.id(), segment.segment_num, other);
                }
            }
        }

        if byte_offset == 0 {
            // we weren't able to append to the last segment, so we need to create a new one
            segment_num = self.segments.front().map(|s: &Segment| {
                s.segment_num.next()
            }).unwrap_or(FIRST_SEGMENT_NUM);

            let segment_end_time = time::now() + self.max_segment_duration;
            let new_segment = Segment::init_new(&self.partition_dir,
                                                segment_num,
                                                self.max_segment_size,
                                                segment_end_time)?;
            self.reader_refs.add(new_segment.range_iter(0));
            self.segments.push_front(new_segment);

            match self.segments.front_mut().unwrap().append(event) {
                AppendResult::Success(offset) => {
                    byte_offset = offset;
                }
                AppendResult::IoError(kind) => {
                    return Err(kind.into());
                }
                other @ _ => {
                    error!("Event {} can't fit into new segment: {:?} due to: {:?}", event.id(), segment_num, other);
                    return Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", other)));
                }
            }
        }

        let index_entry = IndexEntry {
            counter: event.id().event_counter,
            segment: segment_num,
            file_offset: byte_offset,
        };
        self.index.append(index_entry);
        Ok(())
    }

    pub fn fsync(&mut self) -> io::Result<()> {
        for segment in self.segments.iter_mut() {
            segment.fsync()?
        }
        Ok(())
    }

    fn current_segment_num(&self) -> SegmentNum {
        self.segments.front().map(|s| s.segment_num).unwrap_or(SegmentNum(0))
    }

    fn handle_consume(&mut self, connection_id: ConnectionId, consume: ConsumeOperation) -> io::Result<()> {
        let ConsumeOperation {client_sender, filter, start_exclusive, notifier} = consume;
        let reader = self.create_reader(connection_id, filter, start_exclusive);

        // We don't really care if the receiving end has hung up already
        // but we don't want to actually add the notifier to the consumer manager in that case
        let result = client_sender.send(reader);
        if let Ok(_) = result {
            self.consumer_manager.add_uncommitted(notifier);
        }
        Ok(())
    }

    fn create_reader(&mut self, connection_id: ConnectionId, filter: EventFilter, start_exclusive: EventCounter) -> PartitionReader {
        let current_segment_num = self.current_segment_num();
        let index_entry: Option<IndexEntry> = self.index.get_next_entry(start_exclusive);
        let readers = self.reader_refs.get_reader_refs();

        let current_segment = match index_entry {
            Some(entry) => {
                readers.get_next_segment(entry.segment.previous()).map(|mut segment| {
                    segment.set_offset(entry.file_offset);
                    segment
                })
            }
            None => {
                // The requested event counter comes after the end of the stream, so just return the current segment
                let mut reader = readers.get_segment(current_segment_num);
                reader.as_mut().map(|r| r.set_offset_to_end());
                reader
            }
        };

        let commit_index_reader = self.partition_highest_committed.reader();
        PartitionReader::new(connection_id,
                             self.partition_num,
                             filter,
                             current_segment,
                             self.reader_refs.get_reader_refs(),
                             commit_index_reader)
    }


}

#[derive(Debug, PartialEq)]
struct EventToProduce {
    id: FloEventId,
    ts: Timestamp,
    produce: ProduceEvent,
}

impl FloEvent for EventToProduce {
    fn id(&self) -> &FloEventId {
        &self.id
    }

    fn timestamp(&self) -> Timestamp {
        self.ts
    }

    fn parent_id(&self) -> Option<FloEventId> {
        self.produce.parent_id
    }

    fn namespace(&self) -> &str {
        &self.produce.namespace
    }

    fn data_len(&self) -> u32 {
        self.produce.data.len() as u32
    }

    fn data(&self) -> &[u8] {
        &self.produce.data
    }
}


#[cfg(test)]
mod test {
    use chrono::Duration;
    use tempdir::TempDir;
    use futures::sync::oneshot;

    use super::*;
    use protocol::ProduceEvent;
    use engine::event_stream::partition::{ProduceOperation, EventFilter, PartitionReader};
    use engine::event_stream::{EventStreamOptions, HighestCounter};
    use engine::ConnectionId;
    use atomics::AtomicBoolWriter;

    const PARTITION_NUM: ActorId = 1;
    const CONNECTION: ConnectionId = 55;

    #[test]
    fn partition_impl_integration_test() {
        let _ = ::env_logger::init();

        let status = AtomicBoolWriter::with_value(true);
        let options = EventStreamOptions {
            name: "superduper".to_owned(),
            num_partitions: 1,
            event_retention: Duration::seconds(20),
            max_segment_duration: Duration::seconds(5),
            segment_max_size_bytes: 256,
        };
        let tempdir = TempDir::new("partition_persist_events_and_read_them_back").unwrap();

        // Init a new partition and append a bunch of events in two groups
        {
            let mut partition = PartitionImpl::init_new(PARTITION_NUM,
                                                        tempdir.path().to_owned(),
                                                        &options,
                                                        status.reader(),
                                                        HighestCounter::zero()).unwrap();

            let (client_tx, _client_rx) = oneshot::channel();

            let produce = ProduceOperation {
                client: client_tx,
                op_id: 3,
                events: vec![
                    ProduceEvent {
                        op_id: 3,
                        partition: PARTITION_NUM,
                        namespace: "/foo/bar".to_owned(),
                        parent_id: None,
                        data: "the quick".to_owned().into_bytes(),
                    },
                    ProduceEvent {
                        op_id: 3,
                        partition: PARTITION_NUM,
                        namespace: "/foo/bar".to_owned(),
                        parent_id: None,
                        data: "brown fox".to_owned().into_bytes(),
                    }
                ],
            };

            partition.handle_produce(produce).unwrap();

            let mut reader: PartitionReader = partition.create_reader(CONNECTION, EventFilter::All, 0);
            let event = reader.read_next_uncommitted().expect("read_next returned None").expect("read_next returned error");
            assert_eq!(b"the quick", event.data());
            let event2 = reader.read_next_uncommitted().expect("read_next returned None").expect("read_next returned error");
            assert_eq!(b"brown fox", event2.data());
            assert!(reader.next().is_none());

            let moar_events = (0..100).map(|_| {
                ProduceEvent {
                    op_id: 4,
                    partition: PARTITION_NUM,
                    namespace: "/boo/hoo".to_owned(),
                    parent_id: None,
                    data: "stew".to_owned().into_bytes()
                }
            }).collect::<Vec<_>>();

            let (client_tx, _client_rx) = oneshot::channel();

            partition.handle_produce(ProduceOperation {
                client: client_tx,
                op_id: 4,
                events: moar_events,
            }).expect("failed to persist large batch");

            let mut total = 0;
            for result in reader {
                result.expect("failed to read event");
                total += 1;
            }
            assert_eq!(100, total);
            partition.fsync().expect("failed to fsync");
        }

        // now try to initialize the partition from an existing file
        let result = PartitionImpl::init_existing(PARTITION_NUM, tempdir.path().to_owned(), &options, status.reader(), HighestCounter::zero());
        let mut partition = result.expect("Failed to init partitionImpl");

        let reader = partition.create_reader(77, EventFilter::All, 0);
        let count = reader.map(|read_result| {
            read_result.expect("failed to read event after re-init");
        }).count();
        assert_eq!(102, count);
    }
}
