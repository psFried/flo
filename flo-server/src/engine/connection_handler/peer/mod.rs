mod peer_follower;

use protocol::{ProtocolMessage, PeerAnnounce, EventStreamStatus, ClusterMember};
use engine::{ReceivedProtocolMessage, ConnectionId};
use engine::controller::SystemStreamRef;
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
        state.send_to_client(protocol_message).expect("failed to send peer announce when establishing outgoing connection");

        *self = PeerConnectionState::AwaitingPeerResponse;
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

    pub fn message_received(&mut self, message: ReceivedProtocolMessage, state: &mut ConnectionState) -> ConnectionHandlerResult {

        match message {
            ProtocolMessage::StreamStatus(status) => {
                self.stream_status_received(state, status);
            }
            other @ _ => {
                error!("Unhandled protocol message from peer: {:?}", other);
            }
        }

        Ok(())
    }

    fn stream_status_received(&mut self, state: &mut ConnectionState, _status: EventStreamStatus) {
        self.set_state(state.connection_id, PeerConnectionState::Peer);
        unimplemented!()
    }

    fn set_state(&mut self, connection_id: ConnectionId, new_state: PeerConnectionState) {
        debug!("Transitioning connection_id: {} from {:?} to {:?}", connection_id, self, new_state);
        *self = new_state;
    }
}
