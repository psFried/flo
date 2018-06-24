use nom::be_u32;

use event::OwnedFloEvent;
use serializer::Serializer;
use super::{parse_str, ProtocolMessage};

pub const CLIENT_ANNOUNCE: u8 = 170;

/// Sent by the client as the very first message to the server. The server will respond with an `EventStreamStatus` for the current (default) stream
#[derive(Debug, PartialEq, Clone)]
pub struct ClientAnnounce {
    pub protocol_version: u32,
    pub op_id: u32,
    pub client_name: String,
    pub consume_batch_size: Option<u32>,
}

named!{pub parse_client_announce<ProtocolMessage<OwnedFloEvent>>, chain!(
    _tag: tag!(&[CLIENT_ANNOUNCE]) ~
    protocol_version: be_u32 ~
    op_id: be_u32 ~
    client_name: parse_str ~
    batch_size: be_u32,
    || {
        let batch = if batch_size > 0 { Some(batch_size) } else { None };

        ProtocolMessage::Announce(ClientAnnounce{
            protocol_version: protocol_version,
            op_id: op_id,
            client_name: client_name,
            consume_batch_size: batch
        })
    }
)}

pub fn serialize_client_announce(announce: &ClientAnnounce, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(CLIENT_ANNOUNCE)
            .write_u32(announce.protocol_version)
            .write_u32(announce.op_id)
            .write_string(&announce.client_name)
            .write_u32(announce.consume_batch_size.unwrap_or(0))
            .finish()
}
