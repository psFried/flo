
use nom::{be_u64, be_u32};

use event::{OwnedFloEvent, FloEventId};
use serializer::Serializer;
use super::{ProtocolMessage, parse_str, parse_version_vec};


pub const NEW_START_CONSUMING: u8 = 17;

pub const CONSUME_UNLIMITED: u64 = 0;

/// New message sent from client to server to begin reading events from the stream
#[derive(Debug, PartialEq, Clone)]
pub struct NewConsumerStart {
    pub op_id: u32,
    pub version_vector: Vec<FloEventId>,
    pub max_events: u64,
    pub namespace: String,
}

named!{pub parse_new_start_consuming<ProtocolMessage<OwnedFloEvent>>,
    chain!(
        _tag: tag!(&[NEW_START_CONSUMING]) ~
        op_id: be_u32 ~
        version_vec: parse_version_vec ~
        max_events: be_u64 ~
        namespace: parse_str,
        || {
            ProtocolMessage::NewStartConsuming(NewConsumerStart {
                op_id: op_id,
                version_vector: version_vec,
                max_events: max_events,
                namespace: namespace,
            })
        }
    )
}

pub fn serialize_consumer_start(start: &NewConsumerStart, buf: &mut [u8]) -> usize {
    let mut serializer = Serializer::new(buf).write_u8(NEW_START_CONSUMING)
                                             .write_u32(start.op_id)
                                             .write_u16(start.version_vector.len() as u16);

    for id in start.version_vector.iter() {
        serializer = serializer.write_u64(id.event_counter).write_u16(id.actor);
    }
    serializer.write_u64(start.max_events)
              .write_string(&start.namespace).finish()
}




