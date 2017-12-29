mod peer_follower;

use protocol::{ProtocolMessage, PeerAnnounce, EventStreamStatus, ClusterMember};
use engine::{ReceivedProtocolMessage, ConnectionId};
use engine::controller::SystemStreamRef;
use engine::controller::Peer;
use super::connection_state::ConnectionState;
use super::ConnectionHandlerResult;


#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeerConnectionState {
    Init,
    AwaitingPeerResponse,
    Peer,
}


impl PeerConnectionState {
    pub fn initiate_outgoing_peer_connection(&mut self, state: &mut ConnectionState) {
        assert_eq!(PeerConnectionState::Init, *self);
        state.set_to_system_stream();

        let announce = PeerConnectionState::create_peer_announce(&*state.engine.system_stream());
        let protocol_message = ProtocolMessage::PeerAnnounce(announce);
        // safe unwrap since this is called only when creating a brand new outgoing connection
        state.send_to_client(protocol_message).expect("failed to send peer announce when establishing outgoing connection");

        *self = PeerConnectionState::AwaitingPeerResponse;
    }

    pub fn peer_announce_received(&mut self, announce: PeerAnnounce, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let connection_id = state.connection_id;
        let old_state = self.set_state(connection_id, PeerConnectionState::Peer);

        debug!("PeerAnnounce received on connection_id: {}, {:?}, prev_connection_state: {:?}", connection_id, announce, old_state);

        match old_state {
            PeerConnectionState::Peer => {
                // we've already gone through this, so something's wrong
                let message = format!("received redundant PeerAnnounce for connection_id: {}, closing connection", connection_id);
                return Err(message)
            }
            PeerConnectionState::Init => {
                // This was an incoming connection, and this was the first peer message sent, so we need to respond in kind
                let peer_announce = PeerConnectionState::create_peer_announce(state.get_system_stream());
                state.send_to_client(ProtocolMessage::PeerAnnounce(peer_announce))?;

            }
            PeerConnectionState::AwaitingPeerResponse => {

            }
        }
        state.set_to_system_stream();

        let PeerAnnounce {instance_id, system_primary_id, cluster_members, ..} = announce;
        let primary = system_primary_id.and_then(|primary_id| {
            cluster_members.iter().find(|member| {
                member.id == primary_id
            }).map(|member| {
                Peer {
                    id: member.id,
                    address: member.address,
                }
            })
        });
        let peers = cluster_members.into_iter().map(|member| {
            member_to_peer(member)
        }).collect();
        state.get_system_stream().connection_upgraded_to_peer(connection_id, announce.instance_id, primary, peers);
        Ok(())
    }

    fn create_peer_announce(system_stream: &SystemStreamRef) -> PeerAnnounce {
        system_stream.with_cluster_state(|state| {
            let instance_id = state.this_instance_id;
            let address = state.this_address.expect("Attempted to send PeerAnnounce, but system is not in cluster mode");
            let primary = state.system_primary.as_ref().map(|peer| peer.id);
            let members = state.peers.iter().map(|peer| {
                ClusterMember {
                    id: peer.id,
                    address: peer.address,
                }
            }).collect::<Vec<_>>();

            PeerAnnounce {
                protocol_version: 1,
                instance_id,
                peer_address: address,
                op_id: 1,
                system_primary_id: primary,
                cluster_members: members,
            }
        })
    }

    fn set_state(&mut self, connection_id: ConnectionId, new_state: PeerConnectionState) -> PeerConnectionState {
        debug!("Transitioning connection_id: {} from {:?} to {:?}", connection_id, self, new_state);
        ::std::mem::replace(self, new_state)
    }
}

fn member_to_peer(ClusterMember{id, address}: ClusterMember) -> Peer {
    Peer { id, address }
}

#[cfg(test)]
mod test {

}
