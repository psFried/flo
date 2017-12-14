mod peer_follower;

use protocol::{ProtocolMessage, PeerAnnounce, EventStreamStatus};
use engine::{ReceivedProtocolMessage, ConnectionId};
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

        let this_address = state.engine.get_this_instance_address()
                .expect("Tried to initiate outgoing connection but this_instance_address is None");
        let announce = PeerAnnounce {
            protocol_version: 1,
            peer_address: this_address,
            op_id: 1,
        };
        let protocol_message = ProtocolMessage::PeerAnnounce(announce);
        state.send_to_client(protocol_message).expect("failed to send peer announce when establishing outgoing connection");

        *self = PeerConnectionState::AwaitingPeerResponse;
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
