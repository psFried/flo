
use std::io;
use std::collections::VecDeque;
use std::path::PathBuf;

use event::ActorId;
use super::{SharedReaderRefsMut, Operation, OpType, ProduceOperation, ConsumeOperation, PartitionReader, EventFilter};
use super::segment::{Segment, SegmentReader, PersistentEvent};
use super::index::{PartitionIndex, IndexEntry};

pub struct PartitionImpl {
    event_stream_name: String,
    partition_num: ActorId,
    partition_dir: PathBuf,
    segments: VecDeque<Segment>,
    index: PartitionIndex,

    /// new segments each have a reader added here. The readers are then accessed as needed by the EventReader
    reader_refs: SharedReaderRefsMut,
}

impl PartitionImpl {

    pub fn process(&mut self, operation: Operation) {
        let Operation{client_message_recv_time, connection_id, op_type} = operation;

        match op_type {
            OpType::Produce(produce_op) => {

            }
            OpType::Consume(consume_op) => {

            }
        }

        unimplemented!()
    }


    fn produce(&mut self, produce: ProduceOperation) -> io::Result<()> {
        unimplemented!()
    }

    fn handle_consume(&mut self, consume: ConsumeOperation) -> io::Result<()> {
        let index_entry: Option<IndexEntry> = self.index.get_next_entry(consume.start_exclusive);
        let readers = self.reader_refs.get_reader_refs();
        let current_segment = index_entry.and_then(|entry| {
            readers.get_segment(entry.segment).map(|mut segment| {
                segment.set_offset(entry.file_offset);
                segment
            })
        });
        unimplemented!()
    }

}
