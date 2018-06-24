
use nom::{be_u64, be_u32, be_u16};
use serializer::Serializer;
use event::{FloEventId, OwnedFloEvent};
use super::ProtocolMessage;

pub const ACK_HEADER: u8 = 9;


/// Sent by the server to the producer of an event to acknowledge that the event was successfully persisted to the stream.
#[derive(Debug, PartialEq, Clone)]
pub struct EventAck {
    /// This will be set to the `op_id` that was sent in the `ProduceEventHeader`
    pub op_id: u32,

    /// The id that was assigned to the event. This id is immutable and must be the same across all servers in a flo cluster.
    pub event_id: FloEventId,
}

named!{pub parse_event_ack<ProtocolMessage<OwnedFloEvent>>,
    chain!(
        _tag: tag!(&[ACK_HEADER]) ~
        op_id: be_u32 ~
        counter: be_u64 ~
        actor_id: be_u16,
        || {
            ProtocolMessage::AckEvent(EventAck {
                op_id: op_id,
                event_id: FloEventId::new(actor_id, counter)
            })
        }
    )
}


pub fn serialize_event_ack(ack: &EventAck, buf: &mut [u8]) -> usize {
    Serializer::new(buf).write_u8(ACK_HEADER)
                        .write_u32(ack.op_id)
                        .write_u64(ack.event_id.event_counter)
                        .write_u16(ack.event_id.actor)
                        .finish()
}

