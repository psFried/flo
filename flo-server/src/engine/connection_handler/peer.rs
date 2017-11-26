
use protocol::{ProtocolMessage, EventStreamStatus};
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
        let system_stream_state = state.get_current_stream_status(1);
        let peer_announce = ProtocolMessage::PeerAnnounce(system_stream_state);
        state.send_to_client(peer_announce).expect("failed to send peer announce when establishing outgoing connection");

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

    fn stream_status_received(&mut self, state: &mut ConnectionState, status: EventStreamStatus) {
        self.set_state(state.connection_id, PeerConnectionState::Peer);



    }

    fn set_state(&mut self, connection_id: ConnectionId, new_state: PeerConnectionState) {
        debug!("Transitioning connection_id: {} from {:?} to {:?}", connection_id, self, new_state);
        *self = new_state;
    }
}
