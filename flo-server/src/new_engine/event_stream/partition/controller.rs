
use std::io;
use std::collections::VecDeque;
use std::path::PathBuf;

use chrono::Duration;

use protocol::ProduceEvent;
use event::{ActorId, FloEventId, OwnedFloEvent, EventCounter, FloEvent, Timestamp, time};
use super::{SharedReaderRefsMut, Operation, OpType, ProduceOperation, ConsumeOperation, PartitionReader, EventFilter, SegmentNum};
use super::segment::{Segment, SegmentReader, PersistentEvent};
use super::index::{PartitionIndex, IndexEntry};
use new_engine::event_stream::EventStreamOptions;
use new_engine::ConnectionId;

const FIRST_SEGMENT_NUM: SegmentNum = SegmentNum(1);

pub struct PartitionImpl {
    event_stream_name: String,
    partition_num: ActorId,
    partition_dir: PathBuf,
    max_segment_size: usize,
    max_segment_duration: Duration,
    segments: VecDeque<Segment>,
    index: PartitionIndex,
    greatest_event_id: EventCounter,

    /// new segments each have a reader added here. The readers are then accessed as needed by the EventReader
    reader_refs: SharedReaderRefsMut,
}

impl PartitionImpl {

    pub fn init_new(partition_num: ActorId, partition_data_dir: PathBuf, options: &EventStreamOptions) -> io::Result<PartitionImpl> {
        Ok(PartitionImpl {
            event_stream_name: options.name.to_owned(),
            partition_num: partition_num,
            partition_dir: partition_data_dir,
            max_segment_duration: options.max_segment_duration,
            max_segment_size: options.segment_max_size_bytes,
            segments: VecDeque::with_capacity(4),
            index: PartitionIndex::new(partition_num),
            greatest_event_id: 0,
            reader_refs: SharedReaderRefsMut::new(),
        })
    }

    pub fn process(&mut self, operation: Operation) -> io::Result<()> {
        let Operation{client_message_recv_time, connection_id, op_type} = operation;

        match op_type {
            OpType::Produce(produce_op) => {
                self.handle_produce(produce_op)
            }
            OpType::Consume(consume_op) => {
                self.handle_consume(connection_id, consume_op)
            }
        }
    }


    fn handle_produce(&mut self, produce: ProduceOperation) -> io::Result<()> {
        let ProduceOperation {client, op_id, events} = produce;
        let timestamp = time::now();
        for produce_event in events {
            self.greatest_event_id += 1;
            let event = EventToProduce {
                id: FloEventId::new(self.partition_num, self.greatest_event_id),
                ts: timestamp,
                produce: produce_event,
            };
            // early return if creating segment fails or if appending fails
            self.append(&event)?;
        }
        Ok(())
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

            let end_time = time::now() + self.max_segment_duration;
            let new_segment = Segment::init_new(&self.partition_dir,
                                                segment_num,
                                                self.max_segment_size,
                                                end_time)?;
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

    fn current_segment_num(&self) -> SegmentNum {
        self.segments.front().map(|s| s.segment_num).unwrap_or(SegmentNum(0))
    }

    fn handle_consume(&mut self, connection_id: ConnectionId, consume: ConsumeOperation) -> io::Result<()> {
        let ConsumeOperation {client_sender, filter, start_exclusive} = consume;
        let reader = self.create_reader(connection_id, filter, start_exclusive);
        let _ = client_sender.complete(reader);
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
                }).unwrap()
            }
            None => {
                // The requested event counter comes after the end of the stream, so just return the current segment
                let mut reader = readers.get_segment(current_segment_num).unwrap(); // safe unwrap since it's the current segment
                reader.set_offset_to_end();
                reader
            }
        };

        PartitionReader::new(connection_id, self.partition_num, filter, current_segment, self.reader_refs.get_reader_refs())
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

    use super::*;
    use event::{FloEventId, ActorId};
    use protocol::ProduceEvent;
    use new_engine::event_stream::partition::{SharedReaderRefsMut, Operation, OpType, ProduceOperation, ConsumeOperation, EventFilter, PartitionReader};
    use new_engine::event_stream::EventStreamOptions;
    use new_engine::{ConnectionId, create_client_channels};

    const PARTITION_NUM: ActorId = 1;
    const CONNECTION: ConnectionId = 55;

    #[test]
    fn persist_events_and_read_them_back() {
        let options = EventStreamOptions {
            name: "superduper".to_owned(),
            num_partitions: 1,
            event_retention: Duration::seconds(20),
            max_segment_duration: Duration::seconds(5),
            segment_max_size_bytes: 256,
        };
        let tempdir = TempDir::new("partition_persist_events_and_read_them_back").unwrap();

        let mut partition = PartitionImpl::init_new(PARTITION_NUM,
                                                tempdir.path().to_owned(),
                                                &options).unwrap();

        let (client_tx, client_rx) = create_client_channels();

        let produce = ProduceOperation {
            client: client_tx.clone(),
            op_id: 3,
            events: vec![
                ProduceEvent {
                    op_id: 3,
                    namespace: "/foo/bar".to_owned(),
                    parent_id: None,
                    data: "the quick".to_owned().into_bytes(),
                },
                ProduceEvent {
                    op_id: 3,
                    namespace: "/foo/bar".to_owned(),
                    parent_id: None,
                    data: "brown fox".to_owned().into_bytes(),
                }
            ],
        };

        partition.handle_produce(produce).unwrap();

        {
            let mut reader: PartitionReader = partition.create_reader(CONNECTION, EventFilter::All, 0);
            let event = reader.read_next().expect("read_next returned None").expect("read_next returned error");
            assert_eq!(b"the quick", event.data());
            let event2 = reader.read_next().expect("read_next returned None").expect("read_next returned error");
            assert_eq!(b"brown fox", event2.data());
        }

        let moar_events = (0..100).map(|i| {
            ProduceEvent {
                op_id: 4,
                namespace: "/boo/hoo".to_owned(),
                parent_id: None,
                data: "stew".to_owned().into_bytes()
            }
        }).collect::<Vec<_>>();

        partition.handle_produce(ProduceOperation{
            client: client_tx.clone(),
            op_id: 4,
            events: moar_events,
        }).expect("failed to persist large batch");

        let mut reader = partition.create_reader(CONNECTION, EventFilter::All, 0);

        let mut total = 0;
        for result in reader {
            result.expect("failed to read event");
            total += 1;
        }
        assert_eq!(102, total);
    }
}
