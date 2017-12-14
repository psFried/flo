
use std::net::SocketAddr;
use nom::{be_u32, be_u16};
use serializer::{Serializer, FloSerialize};
use event::OwnedFloEvent;
use super::{parse_socket_addr, ProtocolMessage, FloInstanceId};
use super::flo_instance_id::{parse_flo_instance_id, parse_optional_flo_instance_id};

pub const PEER_ANNOUNCE: u8 = 7;

/// Sent by one server to another as the very first message to announce its presence
#[derive(Debug, PartialEq, Clone)]
pub struct PeerAnnounce {
    pub protocol_version: u32,
    pub peer_address: SocketAddr,
    pub op_id: u32,
    pub instance_id: FloInstanceId,
    pub system_primary_id: Option<FloInstanceId>,
    pub cluster_members: Vec<ClusterMember>
}

#[derive(Debug, PartialEq, Clone)]
pub struct ClusterMember {
    pub id: FloInstanceId,
    pub address: SocketAddr,
}

impl FloSerialize for ClusterMember {
    fn serialize<'a>(&'a self, serializer: Serializer<'a>) -> Serializer<'a> {
        serializer.write(&self.id).write_socket_addr(self.address)
    }
}

named!{parse_cluster_member<ClusterMember>, do_parse!(
    id: parse_flo_instance_id  >>
    address: parse_socket_addr >>

    ( ClusterMember{id, address} )
)}

named!{pub parse_peer_announce<ProtocolMessage<OwnedFloEvent>>,
    do_parse!(
        tag!(&[PEER_ANNOUNCE]) >>
        protocol_version: be_u32 >>
        op_id: be_u32 >>
        instance_id: parse_flo_instance_id >>
        peer_address: parse_socket_addr >>
        system_primary_id: parse_optional_flo_instance_id >>
        cluster_members: length_count!(be_u16, parse_cluster_member) >>

        ( ProtocolMessage::PeerAnnounce(PeerAnnounce {
                protocol_version,
                op_id,
                instance_id,
                peer_address,
                system_primary_id,
                cluster_members
            }) )
    )
}

pub fn serialize_peer_announce(announce: &PeerAnnounce, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(PEER_ANNOUNCE)
            .write_u32(announce.protocol_version)
            .write_u32(announce.op_id)
            .write(&announce.instance_id)
            .write_socket_addr(announce.peer_address)
            .write(&announce.system_primary_id.unwrap_or(FloInstanceId::null()))
            .write_u16(announce.cluster_members.len() as u16)
            .write_many(announce.cluster_members.iter(), |ser, member| {
                ser.write(member)
            })
            .finish()

}
