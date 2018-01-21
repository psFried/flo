mod peer_follower;
mod system_read_wrapper;

use std::collections::VecDeque;

use event::{EventCounter, OwnedFloEvent, FloEvent};
use protocol::{self, ProtocolMessage, PeerAnnounce, EventStreamStatus, ClusterMember, ErrorMessage, ErrorKind, FloInstanceId, Term};
use engine::{ReceivedProtocolMessage, ConnectionId};
use engine::controller::{self, SystemStreamRef, Peer, SystemEvent, SystemStreamReader, SYSTEM_READER_BATCH_SIZE};
use engine::event_stream::partition::PersistentEvent;
use super::connection_state::ConnectionState;
use super::{ConnectionHandlerResult, CallAppendEntries, AppendEntriesStart};
use self::system_read_wrapper::SystemReaderWrapper;


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
    system_partition_reader: Option<SystemReaderWrapper>,
    this_peer_info: Option<Peer>,
    in_progress_append: Option<controller::ReceiveAppendEntries>,
}


impl PeerConnectionState {
    pub fn new() -> PeerConnectionState {
        PeerConnectionState {
            state: State::Init,
            current_op_id: 0,
            controller_operation_queue: VecDeque::new(),
            peer_operation_queue: VecDeque::new(),
            system_partition_reader: None,
            this_peer_info: None,
            in_progress_append: None,
        }
    }

    pub fn append_entries_response_received(&mut self, response: protocol::AppendEntriesResponse, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let op_id = self.peer_operation_queue.pop_front().ok_or_else(|| {
            "Received AppendEntriesResponse but no response was expected".to_owned()
        })?;
        if op_id != response.op_id {
            return Err(format!("Expected AppendEntriesResponse with op_id: {}, but received: {:?}", op_id, response));
        }
        let success = if response.success {
            self.system_partition_reader.as_mut().map(|reader| {
                reader.append_acknowledged()
            })
        } else {
            None
        };
        let controller_message = controller::AppendEntriesResponse {
            term: response.term,
            success,
        };
        let connection_id = state.connection_id;
        state.get_system_stream().append_entries_response_received(connection_id, controller_message);
        Ok(())
    }

    pub fn send_append_entries_response(&mut self, response: controller::AppendEntriesResponse, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let op_id = self.controller_operation_queue.pop_front().ok_or_else(|| {
            "send_append_entries_response was called but there was no pending operation".to_owned()
        })?;
        let protocol_message = protocol::AppendEntriesResponse {
            op_id,
            term: response.term,
            success: response.success.is_some(),
        };
        state.send_to_client(ProtocolMessage::SystemAppendResponse(protocol_message))
    }

    pub fn append_entries_received(&mut self, append: protocol::AppendEntriesCall, connection: &mut ConnectionState) -> ConnectionHandlerResult {
        self.ensure_peer_state(Some(append.op_id), connection)?;

        self.controller_operation_queue.push_back(append.op_id);
        let event_count = append.entry_count;
        let controller_message = controller::ReceiveAppendEntries {
            term: append.term,
            prev_entry_index: append.prev_entry_index,
            prev_entry_term: append.prev_entry_term,
            commit_index: append.leader_commit_index,
            events: Vec::with_capacity(event_count as usize),
        };

        if event_count == 0 {
            let connection_id = connection.connection_id;
            connection.get_system_stream().append_entries_received(connection_id, controller_message);
        } else {
            self.in_progress_append = Some(controller_message);
        }
        Ok(())
    }

    pub fn event_received(&mut self, event: OwnedFloEvent, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let connection_id = state.connection_id;
        self.ensure_peer_state(None, state)?;
        if self.in_progress_append.is_none() {
            let error_message = ErrorMessage {
                op_id: 0,
                kind: ErrorKind::InvalidPeerState,
                description: "No event was expected".to_owned(),
            };
            let _ = state.send_to_client(ProtocolMessage::Error(error_message));
            Err(format!("Received event for connection_id: {}, when none was expected", connection_id))
        } else {
            let receive_complete = {
                let in_progress = self.in_progress_append.as_mut().unwrap();
                in_progress.events.push(event);
                in_progress.events.capacity() == in_progress.events.len()
            };
            if receive_complete {
                let append = self.in_progress_append.take().unwrap();
                state.get_system_stream().append_entries_received(connection_id, append);
            }
            Ok(())
        }
    }

    fn ensure_peer_state(&self, incoming_message_op_id: Option<u32>, state: &mut ConnectionState) -> ConnectionHandlerResult {
        if self.state != State::Peer {
            let err = ErrorMessage {
                op_id: incoming_message_op_id.unwrap_or(0),
                kind: ErrorKind::InvalidPeerState,
                description: "No PeerAnnounce message has been received so peer operations are invalid".to_owned(),
            };
            // ignore whatever send error might be raised here, since we're returning an error anyway, which will close the connection
            let _ = state.send_to_client(ProtocolMessage::Error(err));
            Err(format!("Expected connection to be in {:?} state, but was {:?}", State::Peer, self.state))
        } else {
            Ok(())
        }
    }

    pub fn send_append_entries(&mut self, append: CallAppendEntries, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let op_id = self.next_op_id();
        self.peer_operation_queue.push_back(op_id);
        let this_peer = self.get_this_peer_info(state);

        if self.system_partition_reader.is_none() {
            self.system_partition_reader = Some(SystemReaderWrapper::new(state));
        }
        let reader = self.system_partition_reader.as_mut().unwrap();

        reader.send_append_entries(op_id, this_peer.id, append, state)
    }

    fn get_this_peer_info(&mut self, state: &mut ConnectionState) -> Peer {
        if self.this_peer_info.is_none() {
            let peer = state.get_system_stream().with_cluster_state(|cluster_state| {
                Peer {
                    id: cluster_state.this_instance_id,
                    address: cluster_state.this_address.clone().unwrap()
                }
            });
            self.this_peer_info = Some(peer);
        }
        self.this_peer_info.as_ref().cloned().unwrap()
    }

    pub fn vote_response_received(&mut self, response: protocol::RequestVoteResponse, state: &mut ConnectionState) -> ConnectionHandlerResult {
        let connection_id = state.connection_id;
        self.ensure_peer_state(Some(response.op_id), state)?;
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
        let maybe_peer = cluster_members.iter().find(|member| {
            member.id == instance_id
        }).map(|member| member_to_peer(member.clone()) );

        if maybe_peer.is_none() {
            return Err(format!("Received invalid peer announce message. Missing a cluster member for the instance identified as peer: {:?}, in: {:?}", instance_id, cluster_members));
        }

        let peer = maybe_peer.unwrap();
        let primary = system_primary_id.and_then(|primary_id| {
            cluster_members.iter().find(|member| {
                member.id == primary_id
            }).map(|member| member_to_peer(member.clone()))
        });

        let peers = cluster_members.into_iter().map(|member| {
            member_to_peer(member)
        }).collect();
        state.get_system_stream().connection_upgraded_to_peer(connection_id, peer, primary, peers);
        Ok(())
    }

    fn create_peer_announce(&mut self, system_stream: &SystemStreamRef) -> PeerAnnounce {
        let op_id = self.next_op_id();
        system_stream.with_cluster_state(|state| {
            let instance_id = state.this_instance_id;
            let address = state.this_address.expect("Attempted to send PeerAnnounce, but system is not in cluster mode");
            let system_primary_id = state.system_primary.as_ref().map(|peer| peer.id);
            let mut cluster_members = state.peers.iter().map(|peer| {
                ClusterMember {
                    id: peer.id,
                    address: peer.address,
                }
            }).collect::<Vec<_>>();

            // The members enumerated in the shared state do not include an entry for this instance, only the _other_ instances
            // in the cluster. The PeerAnnounce protocol message requires an entry for _every_ member, including this one.
            // So, we always add an entry for ourselves when creating the message
            cluster_members.push(ClusterMember {
                id: instance_id,
                address
            });

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
