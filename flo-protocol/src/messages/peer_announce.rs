
use std::net::SocketAddr;
use nom::be_u32;
use serializer::Serializer;
use event::OwnedFloEvent;
use super::{parse_socket_addr, ProtocolMessage};

pub const PEER_ANNOUNCE: u8 = 7;

/// Sent by one server to another as the very first message to announce its presence
#[derive(Debug, PartialEq, Clone)]
pub struct PeerAnnounce {
    pub protocol_version: u32,
    pub peer_address: SocketAddr,
    pub op_id: u32,
}

named!{pub parse_peer_announce<ProtocolMessage<OwnedFloEvent>>,
    chain!(
        _tag: tag!(&[PEER_ANNOUNCE]) ~
        protocol_version: be_u32 ~
        op_id: be_u32 ~
        peer_address: parse_socket_addr,
        || {
            ProtocolMessage::PeerAnnounce(PeerAnnounce {
                protocol_version,
                op_id,
                peer_address
            })
        }
    )
}

pub fn serialize_peer_announce(announce: &PeerAnnounce, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(PEER_ANNOUNCE)
            .write_socket_addr(announce.peer_address)
            .write_u32(announce.op_id)
            .finish()
}
