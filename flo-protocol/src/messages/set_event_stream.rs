
use nom::be_u32;
use serializer::Serializer;
use event::OwnedFloEvent;
use super::{ProtocolMessage, parse_str};

pub const SET_EVENT_STREAM: u8 = 18;

/// Sent by a client to tell the server which event stream to use for all future operations
#[derive(Debug, PartialEq, Clone)]
pub struct SetEventStream{
    pub op_id: u32,
    pub name: String,
}

named!{pub parse_set_event_stream<ProtocolMessage<OwnedFloEvent>>,
    chain!(
        _tag: tag!(&[SET_EVENT_STREAM]) ~
        op_id: be_u32 ~
        name: parse_str,
        || {
            ProtocolMessage::SetEventStream(SetEventStream {
                op_id: op_id,
                name: name,
            })
        }
    )
}

pub fn serialize_set_event_stream(set_stream: &SetEventStream, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(SET_EVENT_STREAM)
            .write_u32(set_stream.op_id)
            .write_string(&set_stream.name)
            .finish()
}
