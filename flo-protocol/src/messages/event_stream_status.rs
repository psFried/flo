use nom::{be_u64, be_u32, be_u16};

use event::{EventCounter, ActorId, OwnedFloEvent};
use serializer::Serializer;
use super::{parse_str, ProtocolMessage};

pub const EVENT_STREAM_STATUS: u8 = 19;

/// Information on the status of a partition. Included as part of `EventStreamStatus`
#[derive(Debug, PartialEq, Clone)]
pub struct PartitionStatus {
    pub partition_num: ActorId,
    pub head: EventCounter,
    pub primary: bool,
}

/// Contains some basic information on an event stream. Sent in response to a `SetEventStream`
#[derive(Debug, PartialEq, Clone)]
pub struct EventStreamStatus {
    pub op_id: u32,
    pub name: String,
    pub partitions: Vec<PartitionStatus>,
}


named!{parse_partition_status<PartitionStatus>,
    chain!(
        partition_num: be_u16 ~
        head: be_u64 ~
        status_num: be_u16,
        || {
            PartitionStatus {
                partition_num: partition_num,
                head: head,
                primary: status_num == 1,
            }
        }

    )
}

named!{pub parse_event_stream_status<ProtocolMessage<OwnedFloEvent>>,
    chain!(
        _tag: tag!(&[EVENT_STREAM_STATUS]) ~
        status: parse_event_stream_status_struct,
        || {
            ProtocolMessage::StreamStatus(status)
        }
    )
}

named!{parse_event_stream_status_struct<EventStreamStatus>,
    chain!(
        op_id: be_u32 ~
        name: parse_str ~
        partitions: length_count!(be_u16, parse_partition_status),
        || {
            EventStreamStatus {
                op_id: op_id,
                name: name,
                partitions: partitions,
            }
        }
    )
}


pub fn serialize_event_stream_status(status: &EventStreamStatus, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(EVENT_STREAM_STATUS)
            .write_u32(status.op_id)
            .write_string(&status.name)
            .write_u16(status.partitions.len() as u16)
            .write_many(status.partitions.iter(), |ser, partition| {
                let status: u16 = if partition.primary { 1 } else { 0 };
                ser.write_u16(partition.partition_num)
                   .write_u64(partition.head)
                   .write_u16(status)
            })
            .finish()
}
