mod peer_follower;

use std::collections::VecDeque;
use protocol::{self, ProtocolMessage, PeerAnnounce, EventStreamStatus, ClusterMember, ErrorMessage, ErrorKind, FloInstanceId};
use engine::{ReceivedProtocolMessage, ConnectionId};
use engine::controller::{self, SystemStreamRef, Peer, SystemStreamReader};
use super::connection_state::ConnectionState;
use super::{ConnectionHandlerResult, CallAppendEntries};


#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    Init,
    AwaitingPeerResponse,
    Peer,
}

#[derive(Debug)]
pub struct PeerConnectionState {
    state: State,
    current_op_id: u32,
    controller_operation_queue: VecDeque<u32>,
    peer_operation_queue: VecDeque<u32>,
    system_partition_reader: Option<SystemStreamReader>,
    this_instance_id: Option<FloInstanceId>,
}


impl PeerConnectionState {
    pub fn new() -> PeerConnectionState {
        PeerConnectionState {
            state: State::Init,
            current_op_id: 0,
            controller_operation_queue: VecDeque::new(),
            peer_operation_queue: VecDeque::new(),
            system_partition_reader: None,
            this_instance_id: None,
        }
    }

    pub fn send_append_entries(&mut self, append: CallAppendEntries, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let connection_id = state.connection_id;
        let op_id = self.next_op_id();
        let this_instance_id = self.get_this_instance_id(state);
        let CallAppendEntries {current_term, prev_entry_index, prev_entry_term,
            reader_start_offset, reader_start_segment, reader_start_event} = append;

        if self.system_partition_reader.is_none() {
            let reader = state.get_system_stream().create_system_stream_reader(connection_id);
            self.system_partition_reader = Some(reader);
        }

        let reader = self.system_partition_reader.as_mut().unwrap();
        reader.set_to(reader_start_segment, reader_start_offset).map_err(|io_err| {
            format!("Failed to set position of system stream reader for connection_id: {} - {:?}", connection_id ,io_err)
        })?; // just give up

        let num_events = reader.fill_buffer().map_err(|io_err| {
            format!("Failed to rea events for AppendEntries for connection_id: {}, error: {:?}", connection_id, io_err)
        })?; // give up on error

        let append = protocol::AppendEntriesCall {
            op_id,
            leader_id: this_instance_id,
            term: current_term,
            prev_entry_term,
            prev_entry_index,
            leader_commit_index: 0, //TODO: figure out what our current commit_index is
            entry_count: num_events as u32,
        };
        state.send_to_client(ProtocolMessage::SystemAppendCall(append))?;
        for event in reader.drain() {
            state.send_to_client(ProtocolMessage::ReceiveEvent(event))?;
        }
        Ok(())
    }

    fn get_this_instance_id(&mut self, state: &mut ConnectionState) -> FloInstanceId {
        if self.this_instance_id.is_none() {
            let id = state.get_system_stream().with_cluster_state(|cluster_state| cluster_state.this_instance_id);
            self.this_instance_id = Some(id);
        }
        self.this_instance_id.unwrap()
    }

    pub fn vote_response_received(&mut self, response: protocol::RequestVoteResponse, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let connection_id = state.connection_id;
        let expected_op_id = self.peer_operation_queue.pop_back();


        if Some(response.op_id) == expected_op_id {
            let protocol::RequestVoteResponse { op_id, term, vote_granted } = response;
            let controller_message = controller::VoteResponse {
                term,
                granted: vote_granted
            };
            state.get_system_stream().vote_response_received(connection_id, controller_message);
            Ok(())
        } else {
            let err_message =  format!("connection_id: {} received unexpected message: {:?}, expected op_id: {:?}",
                   connection_id, response, expected_op_id);
            error!("{}", err_message);
            Err(err_message)
        }
    }

    pub fn send_vote_response(&mut self, response: controller::VoteResponse, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let pending = self.controller_operation_queue.pop_back();
        if let Some(op_id) = pending {
            let protocol_message = protocol::RequestVoteResponse {
                op_id,
                term: response.term,
                vote_granted: response.granted,
            };
            state.send_to_client(ProtocolMessage::VoteResponse(protocol_message))
        } else {
            let err_message = format!("Refusing to send: {:?} as there is no pending operation", response);
            error!("{}", err_message);
            Err(err_message)
        }
    }

    pub fn request_vote_received(&mut self, request: protocol::RequestVoteCall, state: &mut ConnectionState) -> ConnectionHandlerResult {
        if self.state == State::Peer {
            let protocol::RequestVoteCall { op_id, term, candidate_id, last_log_index, last_log_term } = request;
            self.controller_operation_queue.push_front(op_id);
            let controller_message = controller::CallRequestVote { term, candidate_id, last_log_term, last_log_index };
            let connection_id = state.connection_id;
            state.get_system_stream().request_vote_received(connection_id, controller_message);
            Ok(())
        } else {
            let err_message = format!("Refusing to process RequestVote when connection is in {:?} state", self.state);
            let response = ErrorMessage {
                op_id: request.op_id,
                kind: ErrorKind::InvalidPeerState,
                description: err_message.clone(),
            };
            let _ = state.send_to_client(ProtocolMessage::Error(response));
            // return error so that connection will be closed
            Err(err_message)
        }
    }

    pub fn send_request_vote(&mut self, request: controller::CallRequestVote, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let controller::CallRequestVote { term, candidate_id, last_log_index, last_log_term } = request;
        let op_id = self.next_op_id();
        self.peer_operation_queue.push_front(op_id);
        let protocol_message = protocol::RequestVoteCall {
            op_id,
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        };
        state.send_to_client(ProtocolMessage::RequestVote(protocol_message))
    }

    pub fn initiate_outgoing_peer_connection(&mut self, state: &mut ConnectionState) {
        info!("Upgrading connection_id: {} to outgoing peer-to-peer connection", state.connection_id);
        assert_eq!(State::Init, self.state);
        state.set_to_system_stream();

        let announce = self.create_peer_announce(&*state.engine.system_stream());
        let protocol_message = ProtocolMessage::PeerAnnounce(announce);
        // safe unwrap since this is called only when creating a brand new outgoing connection
        state.send_to_client(protocol_message).expect("failed to send peer announce when establishing outgoing connection");

        self.set_state(state.connection_id, State::AwaitingPeerResponse);
    }

    pub fn peer_announce_received(&mut self, announce: PeerAnnounce, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let connection_id = state.connection_id;
        let old_state = self.set_state(connection_id, State::Peer);

        debug!("PeerAnnounce received on connection_id: {}, {:?}, prev_connection_state: {:?}", connection_id, announce, old_state);
        match old_state {
            State::Peer => {
                // we've already gone through this, so something's wrong
                let message = format!("received redundant PeerAnnounce for connection_id: {}, closing connection", connection_id);
                return Err(message)
            }
            State::Init => {
                // This was an incoming connection, and this was the first peer message sent, so we need to respond in kind
                let peer_announce = self.create_peer_announce(state.get_system_stream());
                state.send_to_client(ProtocolMessage::PeerAnnounce(peer_announce))?;

            }
            State::AwaitingPeerResponse => { }
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

    fn create_peer_announce(&mut self, system_stream: &SystemStreamRef) -> PeerAnnounce {
        let op_id = self.next_op_id();
        system_stream.with_cluster_state(|state| {
            let instance_id = state.this_instance_id;
            let address = state.this_address.expect("Attempted to send PeerAnnounce, but system is not in cluster mode");
            let system_primary_id = state.system_primary.as_ref().map(|peer| peer.id);
            let cluster_members = state.peers.iter().map(|peer| {
                ClusterMember {
                    id: peer.id,
                    address: peer.address,
                }
            }).collect::<Vec<_>>();

            PeerAnnounce {
                op_id,
                instance_id,
                system_primary_id,
                cluster_members,
                protocol_version: 1,
                peer_address: address,
            }
        })
    }

    fn next_op_id(&mut self) -> u32 {
        self.current_op_id += 1;
        self.current_op_id
    }

    fn set_state(&mut self, connection_id: ConnectionId, new_state: State) -> State {
        debug!("Transitioning connection_id: {} from {:?} to {:?}", connection_id, self, new_state);
        ::std::mem::replace(&mut self.state, new_state)
    }
}

fn member_to_peer(ClusterMember{id, address}: ClusterMember) -> Peer {
    Peer { id, address }
}
