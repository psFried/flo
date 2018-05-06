
use nom::be_u32;
use event::{OwnedFloEvent, FloEvent, time};
use serializer::Serializer;
use super::{ProtocolMessage, parse_non_zero_event_id, parse_event_id, parse_timestamp, parse_str};

pub const RECEIVE_EVENT: u8 = 3;

named!{pub parse_receive_event_header<ProtocolMessage<OwnedFloEvent>>,
    chain!(
        _tag: tag!(&[RECEIVE_EVENT]) ~
        crc: be_u32 ~
        id: parse_non_zero_event_id ~
        parent_id: parse_event_id ~
        timestamp: parse_timestamp ~
        namespace: parse_str ~
        data: length_data!(be_u32),
        || {
           ProtocolMessage::ReceiveEvent(OwnedFloEvent {
                id: id,
                parent_id: parent_id,
                namespace: namespace,
                timestamp: timestamp,
                data: data.to_vec(),
                crc: crc
            })
        }
    )
}

pub fn serialize_receive_event_header<E: FloEvent>(event: &E, buf: &mut [u8]) -> usize {
    let crc = event.get_or_compute_crc();
    Serializer::new(buf)
            .write_u8(RECEIVE_EVENT)
            .write_u32(crc)
            .write_u64(event.id().event_counter)
            .write_u16(event.id().actor)
            .write_u64(event.parent_id().map(|id| id.event_counter).unwrap_or(0))
            .write_u16(event.parent_id().map(|id| id.actor).unwrap_or(0))
            .write_u64(time::millis_since_epoch(event.timestamp()))
            .write_string(event.namespace())
            .write_u32(event.data_len())
            .finish()
}
