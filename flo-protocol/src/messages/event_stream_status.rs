use std::net::SocketAddr;

use nom::{be_u64, be_u32, be_u16};

use event::{EventCounter, ActorId, OwnedFloEvent};
use serializer::{Serializer, FloSerialize};
use super::{parse_str, parse_bool, parse_socket_addr, ProtocolMessage};

pub const EVENT_STREAM_STATUS: u8 = 19;

/// Information on the status of a partition. Included as part of `EventStreamStatus`
#[derive(Debug, PartialEq, Clone)]
pub struct PartitionStatus {
    pub partition_num: ActorId,
    pub head: EventCounter,
    pub primary: bool,
    pub primary_server_address: Option<SocketAddr>,
}

/// Contains some basic information on an event stream. Sent in response to a `SetEventStream`
#[derive(Debug, PartialEq, Clone)]
pub struct EventStreamStatus {
    pub op_id: u32,
    pub name: String,
    pub partitions: Vec<PartitionStatus>,
}

named!{parse_opt_socket_addr<Option<SocketAddr>>, alt!(
    do_parse!(
        tag!(&[0]) >>

        ( None )
    ) |
    do_parse!(
        tag!(&[1]) >>
        addr: parse_socket_addr >>

        ( Some(addr) )
    )
)}

named!{parse_partition_status<PartitionStatus>,
    chain!(
        partition_num: be_u16 ~
        head: be_u64 ~
        primary: parse_bool ~
        primary_server_address: parse_opt_socket_addr,
        || {
            PartitionStatus {
                partition_num,
                head,
                primary,
                primary_server_address
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

impl FloSerialize for PartitionStatus {
    fn serialize<'a>(&'a self, serializer: Serializer<'a>) -> Serializer<'a> {
        let ser = serializer.write_u16(self.partition_num)
                .write_u64(self.head)
                .write_bool(self.primary);

        match self.primary_server_address.as_ref() {
            Some(addr) => {
                ser.write_u8(1).write_socket_addr(*addr)
            }
            None => {
                ser.write_u8(0)
            }
        }
    }
}

pub fn serialize_event_stream_status(status: &EventStreamStatus, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(EVENT_STREAM_STATUS)
            .write_u32(status.op_id)
            .write_string(&status.name)
            .write_u16(status.partitions.len() as u16)
            .write_many(status.partitions.iter(), |ser, partition| {
                ser.write(partition)
            })
            .finish()
}
