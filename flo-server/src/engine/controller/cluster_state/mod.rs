pub mod persistent;
mod peer_connections;
mod primary_state;

use std::io;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use std::time::{Instant, Duration};
use std::path::Path;
use std::collections::{HashMap, HashSet};

use event::{EventCounter, ActorId, OwnedFloEvent};
use engine::{ConnectionId, EngineRef};
use engine::connection_handler::ConnectionControl;
use engine::controller::{CallRequestVote, VoteResponse};
use protocol::{FloInstanceId, Term};
use atomics::{AtomicBoolWriter, AtomicBoolReader};
use super::{ClusterOptions, ConnectionRef, Peer, PeerUpgrade, AppendEntriesResponse, ReceiveAppendEntries, ControllerState};
use super::peer_connection::{PeerSystemConnection, OutgoingConnectionCreator, OutgoingConnectionCreatorImpl};
use self::peer_connections::{PeerConnectionManager, PeerConnections};
use self::primary_state::PrimaryState;

pub use self::persistent::{FilePersistedState, PersistentClusterState};

/// This is a placeholder for a somewhat better error handling when updating the persistent cluster state fails.
/// This situation is extremely problematic, since it may be possible to cast two different votes in the same term, if for
/// instance, changes to the `voted_for` field cannot be persisted. For now, we're just going to have the controller panic
/// whenever there's an IO error when persisting cluster state changes. Thankfully, this error should be fairly rare, since
/// the file that's used to persist state changes is opened during initialization.
static STATE_UPDATE_FAILED: &'static str = "failed to persist changes to cluster state! Panicking, since data consistency cannot be guaranteed under these circumstances";


pub trait ConsensusProcessor: Send {
    fn tick(&mut self, now: Instant, controller_state: &mut ControllerState);
    fn is_primary(&self) -> bool;
    fn get_current_term(&self) -> Term;
    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, controller_state: &ControllerState);
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr);
    fn connection_closed(&mut self, connection_id: ConnectionId);

    fn request_vote_received(&mut self, from: ConnectionId, request: CallRequestVote);
    fn vote_response_received(&mut self, now: Instant, from: ConnectionId, response: VoteResponse, controller: &mut ControllerState);

    fn append_entries_received(&mut self, connection_id: ConnectionId, append: ReceiveAppendEntries, controller_state: &mut ControllerState);
    fn append_entries_response_received(&mut self, connection_id: ConnectionId, response: AppendEntriesResponse, controller_state: &mut ControllerState);

    fn send_append_entries(&mut self, controller_state: &mut ControllerState);
}

#[derive(Debug)]
pub struct ClusterManager {
    state: State,
    primary_state: Option<PrimaryState>,
    initialization_peers: HashSet<SocketAddr>,
    this_instance_address: SocketAddr,
    election_timeout: Duration,
    last_heartbeat: Instant,
    primary_status_writer: AtomicBoolWriter,
    last_applied: EventCounter,
    last_applied_term: Term,
    persistent: FilePersistedState,
    votes_received: HashSet<FloInstanceId>,
    system_partition_primary_address: SystemPrimaryAddressRef,
    shared: Arc<RwLock<SharedClusterState>>,
    current_primary: Option<FloInstanceId>,
    connection_manager: Box<PeerConnectionManager>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum State {
    EstablishConnections,
    DeterminePrimary,
    Follower,
    Voted,
    Primary,
}

impl ClusterManager {
    fn new(election_timeout_millis: u64,
           starting_peer_addresses: Vec<SocketAddr>,
           this_instance_address: SocketAddr,
           persistent: FilePersistedState,
           shared: Arc<RwLock<SharedClusterState>>,
           primary_status_writer: AtomicBoolWriter,
           system_partition_primary_address: SystemPrimaryAddressRef,
           peer_connection_manager: Box<PeerConnectionManager>) -> ClusterManager {

        let mut initialization_peers = starting_peer_addresses.iter().cloned().collect::<HashSet<_>>();
        for peer in persistent.cluster_members.iter() {
            initialization_peers.insert(peer.address);
        }

        ClusterManager {
            state: State::EstablishConnections,
            primary_state: None,
            election_timeout: Duration::from_millis(election_timeout_millis),
            last_heartbeat: Instant::now(),
            connection_manager: peer_connection_manager,
            votes_received: HashSet::new(),
            current_primary: None,
            this_instance_address,
            initialization_peers,
            last_applied: 0,
            last_applied_term: 0,
            primary_status_writer,
            persistent,
            system_partition_primary_address,
            shared,
        }
    }

    fn transition_state(&mut self, new_state: State) {
        debug!("Transitioning from state: {:?} to {:?}", self.state, new_state);
        self.state = new_state;
    }

    fn update_last_heartbeat_to_now(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    fn determine_primary_from_peer_upgrade(&mut self, upgrade: PeerUpgrade) {
        let PeerUpgrade { peer, system_primary, .. } = upgrade;
        if let Some(primary) = system_primary {
            info!("Determined primary of {:?} at {}, as told by instance: {:?}", primary.id, primary.address, peer);
            self.transition_state(State::Follower);
            {
                let mut lock = self.system_partition_primary_address.write().unwrap();
                *lock = Some(primary.address);
            }
            {
                let mut shared = self.shared.write().unwrap();
                shared.system_primary = Some(primary);
            }
        } else {
            debug!("peer announce from {:?} has unknown primary", peer);
        }
    }

    fn connection_resolved(&mut self, address: SocketAddr) {
        if self.state == State::DeterminePrimary && self.initialization_peers.remove(&address) {
            if self.initialization_peers.is_empty() {
                warn!("exhausted all peers without determining a primary member");
                self.transition_state(State::Follower);
            }
        }
    }

    fn election_timed_out(&self, now: Instant) -> bool {
        // since `now` comes from the original Tick operation, it's possible that the `last_heartbeat` was since updated
        // and ends up being after `now`. If we don't check for this case, it can cause the controller to panic
        now > self.last_heartbeat &&
                (now - self.last_heartbeat) > self.election_timeout
    }

    fn start_new_election(&mut self) {
        info!("Starting new election with term: {}", self.persistent.current_term + 1);
        self.votes_received.clear();
        let result = self.persistent.modify(|state| {
            state.current_term += 1;
            let my_id = state.this_instance_id;
            state.voted_for = Some(my_id);
        }).expect(STATE_UPDATE_FAILED);

        let connection_control = ConnectionControl::SendRequestVote(CallRequestVote {
            term: self.persistent.current_term,
            candidate_id: self.persistent.this_instance_id,
            last_log_index: self.last_applied,
            last_log_term: self.last_applied_term,
        });
        self.connection_manager.broadcast_to_peers(connection_control);
        self.transition_state(State::Voted);
        // update the timestamp to make sure that we allow the proper amount of time for this election
        self.update_last_heartbeat_to_now();
    }

    fn can_grant_vote(&self, request: &CallRequestVote) -> bool {
        let request_term = request.term;
        let my_term = self.persistent.current_term;

        if request_term < my_term {
            return false;
        } else if request_term == my_term {
            if self.persistent.voted_for == Some(request.candidate_id) {
                return true;
            } else if self.persistent.voted_for.is_some() {
                return false;
            }
        }

        self.persistent.current_term <= request_term &&
                self.last_applied <= request.last_log_index &&
                self.last_applied_term <= request.last_log_term &&
                self.persistent.cluster_members.iter().any(|peer| peer.id == request.candidate_id)
    }

    fn count_vote_response(&mut self, peer_id: FloInstanceId) -> bool {
        if self.votes_received.insert(peer_id) {
            trace!("Counting vote response from peer_id: {:?}", peer_id);
        } else {
            trace!("Not counting duplicate vote from peer_id: {:?}", peer_id);
        }
        let peer_count = self.persistent.cluster_members.len() as ActorId;
        let vote_count = self.votes_received.len() as ActorId;
        let required_count = ::engine::minimum_required_votes_for_majority(peer_count);
        let election_won = vote_count >= required_count;
        debug!("After counting vote from peer_id: {:?}, this instance has {} of {} required votes from a total of {} cluster members; election_won={}",
                peer_id, vote_count, required_count, peer_count, election_won);
        election_won
    }

    fn transition_to_primary(&mut self, controller: &mut ControllerState) {
        info!("Transitioning to system Primary");
        self.transition_state(State::Primary);
        let this_peer = Peer {
            id: self.persistent.this_instance_id,
            address: self.this_instance_address,
        };
        self.set_new_primary(Some(this_peer));
        self.primary_status_writer.set(true);
        let primary_state = PrimaryState::new(self.persistent.current_term);
        self.primary_state = Some(primary_state);
        self.send_append_entries(controller);
    }

    fn set_new_primary(&mut self, primary: Option<Peer>) {
        let primary_id = primary.as_ref().map(|p| p.id);
        self.current_primary = primary_id;
        let mut lock = self.shared.write().unwrap();
        lock.system_primary = primary;
    }

    fn set_follower_status(&mut self, primary: Option<Peer>) {
        info!("Transitioning to follower state with new primary: {:?}", primary);
        self.primary_status_writer.set(false);
        self.primary_state = None;
        self.set_new_primary(primary);
        self.transition_state(State::Follower);
    }

    fn append_system_events(&mut self, events: &[OwnedFloEvent]) -> io::Result<EventCounter> {
        // TODO: actually persist the system events from ApendEntries
        if !events.is_empty() {
            error!("Appending of system events was never implemented!!!, ignoring: {:?}", events);
        }
        Ok(0)
    }

    fn create_append_result(&mut self, events: &[OwnedFloEvent]) -> Option<EventCounter> {
        trace!("AppendEntries looks successful, will append: {} entries", events.len());
        self.append_system_events(events).map(|last_counter| {
            Some(last_counter)
        }).unwrap_or_else(|io_err| {
            error!("Error appending system events: {:?}, returning false", io_err);
            None
        })
    }
}

impl ConsensusProcessor for ClusterManager {

    fn send_append_entries(&mut self, controller_state: &mut ControllerState) {
        let ClusterManager { ref mut primary_state, ref mut connection_manager, ref persistent, ..} = *self;

        primary_state.as_mut().map(|state| {
            let all_peers = persistent.cluster_members.iter().map(|peer| peer.id);
            state.send_append_entries(controller_state, connection_manager.as_mut(), all_peers);
        });
    }

    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr) {
        self.connection_manager.outgoing_connection_failed(connection_id, address);
        self.connection_resolved(address);
    }

    fn connection_closed(&mut self, connection_id: ConnectionId) {
        self.connection_manager.connection_closed(connection_id);
    }

    fn request_vote_received(&mut self, from: ConnectionId, request: CallRequestVote) {
        let candidate_id = request.candidate_id;

        let response = if self.can_grant_vote(&request) {
            self.persistent.modify(|state| {
                state.voted_for = Some(candidate_id);
                state.current_term = request.term;
            }).expect(STATE_UPDATE_FAILED);

            VoteResponse { term: self.persistent.current_term, granted: true }
        } else {
            VoteResponse { term: self.persistent.current_term, granted: false, }
        };

        debug!("Got request vote from connection_id: {}: {:?} - sending response: {:?}", from, request, response);
        self.connection_manager.send_to_peer(candidate_id, ConnectionControl::SendVoteResponse(response));
    }

    fn vote_response_received(&mut self, now: Instant, from: ConnectionId, response: VoteResponse, controller: &mut ControllerState) {
        let peer_id = self.connection_manager.get_peer_id(from);
        if peer_id.is_none() {
            error!("Ignoring Vote Response from connection_id: {}: {:?}", from, response);
            return;
        }
        let peer_id = peer_id.unwrap();

        if response.granted {
            if self.count_vote_response(peer_id) {
                self.transition_to_primary(controller);
            }
        } else {
            let response_term = response.term;
            debug!("VoteResponse from {:?} was not granted, response term: {}, current_term: {}", peer_id, response_term, self.persistent.current_term);
            if response_term > self.persistent.current_term {
                info!("Received response term of {} from peer: {:?}, which is greater than current term of: {}. Updating current term and clearing out {} existing votes",
                        response_term, peer_id, self.persistent.current_term, self.votes_received.len());
                self.persistent.modify(|state| {
                    state.current_term = response_term;
                }).expect(STATE_UPDATE_FAILED);
                self.votes_received.clear();
                self.transition_state(State::Follower);
            }
        }
    }

    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, controller_state: &ControllerState) {
        debug!("peer_connection_established: connection_id: {},  {:?}", connection_id, upgrade);
        let connection = controller_state.get_connection(connection_id);
        if connection.is_none() {
            debug!("Ignoring peer_connection_established: {:?} for connection_id: {}, because that connection has already been closed",
                   upgrade, connection_id);
            return;
        }
        let connection = connection.unwrap();

        if !self.persistent.cluster_members.contains(&upgrade.peer) {
            self.persistent.modify(|state| {
                state.cluster_members.insert(upgrade.peer.clone());
            }).expect(STATE_UPDATE_FAILED);

            let mut lock = self.shared.write().unwrap();
            lock.peers.insert(upgrade.peer.clone());
        }

        self.connection_manager.peer_connection_established(upgrade.peer.clone(), connection);

        if State::DeterminePrimary == self.state {
            self.determine_primary_from_peer_upgrade(upgrade);
        }
        self.connection_resolved(connection.remote_address);
    }

    fn tick(&mut self, now: Instant, controller_state: &mut ControllerState) {
        self.connection_manager.establish_connections(now, controller_state);

        match self.state {
            State::EstablishConnections => {
                // we'll only be in this initial status once, on startup
                self.transition_state(State::DeterminePrimary);
            }
            State::DeterminePrimary => {
                trace!("Waiting to DeterminePrimary");
            }
            State::Primary => {
                trace!("This instance is primary");
                self.send_append_entries(controller_state);
            }
            other @ _ => {
                if self.election_timed_out(now) {
                    self.start_new_election();
                }
            }
        }
    }

    fn is_primary(&self) -> bool {
        self.state == State::Primary
    }

    fn append_entries_received(&mut self, connection_id: ConnectionId, append: ReceiveAppendEntries, controller_state: &mut ControllerState) {
        let from = self.connection_manager.get_peer_id(connection_id);
        if from.is_none() {
            error!("Received AppendEntries from connection_id: {}, which is not a peer connection, received: {:?}", connection_id, append);
            return;
        }
        let peer_id = from.unwrap();
        let my_current_term = self.persistent.current_term;

        // Check if this message indicates a change of system primary
        if self.current_primary.map(|current| current != peer_id).unwrap_or(true) {
            // this message indicates a new primary, so we need to transition
            // safe unwrap here since we are just receiving a message from this connection
            let peer_address = controller_state.get_connection(connection_id).map(|conn| conn.remote_address).unwrap();
            self.set_follower_status(Some(Peer {
                id: peer_id,
                address: peer_address
            }));
        }

        self.update_last_heartbeat_to_now();

        let response = match controller_state.get_next_event(append.prev_entry_index.saturating_sub(1)) {
            Some(Ok((actual_prev_index, actual_prev_term))) => {
                if actual_prev_index == append.prev_entry_index && actual_prev_term == append.prev_entry_term {
                    self.create_append_result(append.events.as_slice())
                } else {
                    info!("AppendEntries: {:?} does not match the prev index/term stored for this instance: index: {}, term: {}", append, actual_prev_index, actual_prev_term);
                    None
                }
            }
            None => {
                if append.prev_entry_index == 0 && append.prev_entry_term == 0 {
                    // special case for when we're at the very beginning and have literally no events in the log
                    self.create_append_result(append.events.as_slice())
                } else {
                    debug!("System partition with head at: {} is behind AppendEntries with index: {}, term: {}, returning negative result",
                            self.last_applied, append.prev_entry_index, append.prev_entry_term);
                    None
                }
            }
            other @ _ => {
                error!("failed to read previous system event at: {} with err: {:?}", append.prev_entry_index, other);
                None
            }
        };

        self.connection_manager.send_to_peer(peer_id, ConnectionControl::SendAppendEntriesResponse(AppendEntriesResponse {
            term: my_current_term,
            success: response,
        }));
    }

    fn append_entries_response_received(&mut self, connection_id: ConnectionId, response: AppendEntriesResponse, controller_state: &mut ControllerState) {
        let from = self.connection_manager.get_peer_id(connection_id);
        if from.is_none() {
            error!("Received AppendEntriesResponse from connection_id: {}, which is not a peer connection. Received: {:?}", connection_id, response);
            return;
        }
        let peer_id = from.unwrap();
        let response_term = response.term;

        if response_term > self.persistent.current_term {
            // Apparently, we've fallen behind! This instance is no longer primary, so we need to step down if we haven't already
            warn!("Peer: {:?} responded with term: {}, which is higher than the current term: {}. Stepping down as primary",
                  peer_id, response_term, self.persistent.current_term);
            if self.is_primary() {
                self.set_follower_status(None);
            }
            self.persistent.modify(|state| {
                state.current_term = response_term;
            }).expect(STATE_UPDATE_FAILED)
        }

        if response.success.is_some() && self.is_primary() {
            let peer_counter = response.success.unwrap();
            // TODO: confirm entries with partition impl
        }
    }
    fn get_current_term(&self) -> Term {
        self.persistent.current_term
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SharedClusterState {
    pub this_instance_id: FloInstanceId,
    pub this_address: Option<SocketAddr>,
    pub system_primary: Option<Peer>,
    pub peers: HashSet<Peer>,
}

impl SharedClusterState {
    pub fn non_cluster() -> SharedClusterState {
        SharedClusterState {
            this_instance_id: FloInstanceId::generate_new(),
            this_address: None,
            system_primary: None,
            peers: HashSet::new(),
        }
    }

    pub fn this_instance_is_primary(&self) -> bool {
        let this_id = self.this_instance_id;
        self.system_primary.as_ref().map(|primary| {
            primary.id == this_id
        }).unwrap_or(false)
    }
}


pub type SystemPrimaryAddressRef = Arc<RwLock<Option<SocketAddr>>>;
pub type ClusterStateReader = Arc<RwLock<SharedClusterState>>;

pub fn init_cluster_consensus_processor(persistent_state: FilePersistedState,
                                    options: ClusterOptions,
                                    engine_ref: EngineRef,
                                    shared_state_ref: ClusterStateReader,
                                    system_primary: AtomicBoolWriter,
                                    primary_address: SystemPrimaryAddressRef) -> Box<ConsensusProcessor> {

    let ClusterOptions{ peer_addresses, event_loop_handles, election_timeout_millis, this_instance_address, .. } = options;

    let outgoing_connection_creator = OutgoingConnectionCreatorImpl::new(event_loop_handles, engine_ref);
    let peer_connection_manager = PeerConnections::new(peer_addresses.clone(), Box::new(outgoing_connection_creator), &persistent_state.cluster_members);

    let state = ClusterManager::new(election_timeout_millis,
                                    peer_addresses,
                                    this_instance_address,
                                    persistent_state,
                                    shared_state_ref,
                                    system_primary,
                                    primary_address,
                                    Box::new(peer_connection_manager));
    Box::new(state)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::collections::HashSet;
    use tempdir::TempDir;
    use engine::connection_handler::{ConnectionControlSender, ConnectionControlReceiver, ConnectionControl, CallAppendEntries, AppendEntriesStart};
    use test_utils::{addr, expect_future_resolved};
    use engine::event_stream::partition::SegmentNum;
    use engine::controller::peer_connection::{OutgoingConnectionCreator, MockOutgoingConnectionCreator};
    use engine::controller::cluster_state::peer_connections::mock::{MockPeerConnectionManager, Invocation};
    use engine::controller::controller_messages::mock::mock_connection_ref;
    use engine::controller::mock::{MockControllerState, MockSystemEvent};

    #[test]
    fn reverts_to_follower_when_append_entries_response_indicates_term_greater_than_current_term() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:1000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:2000")
        };
        let peer_1_connection = 1;
        let peer_2_connection = 2;
        connection_manager.stub_peer_connection(peer_1_connection, peer_1.id);
        connection_manager.stub_peer_connection(peer_2_connection, peer_2.id);

        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        subject.persistent.modify(|state| {
            state.cluster_members.insert(peer_1.clone());
            state.cluster_members.insert(peer_2.clone());
            state.current_term = 5;
        }).unwrap();
        subject.last_applied = 99;
        subject.last_applied_term = 4;
        subject.state = State::Primary;
        subject.primary_state = Some(PrimaryState::new(5));
        subject.last_heartbeat = start;

        let response = AppendEntriesResponse {
            term: 6,
            success: None,
        };
        let mut controller = MockControllerState::new();
        subject.append_entries_response_received(peer_1_connection, response, &mut controller);

        assert_eq!(State::Follower, subject.state);
        assert!(!subject.is_primary());
        assert!(subject.primary_state.is_none());
        assert_eq!(6, subject.persistent.current_term);
    }

    #[test]
    fn reverts_to_follower_when_append_entries_is_received_from_another_peer() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:1000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:2000")
        };
        let peer_1_connection = 1;
        let peer_2_connection = 2;
        connection_manager.stub_peer_connection(peer_1_connection, peer_1.id);
        connection_manager.stub_peer_connection(peer_2_connection, peer_2.id);

        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        subject.persistent.modify(|state| {
            state.cluster_members.insert(peer_1.clone());
            state.cluster_members.insert(peer_2.clone());
            state.current_term = 5;
        }).unwrap();
        subject.last_applied = 99;
        subject.last_applied_term = 4;
        subject.state = State::Primary;
        subject.primary_state = Some(PrimaryState::new(5));
        subject.last_heartbeat = start;

        let mock_system_events = vec![
            MockSystemEvent {
                id: 1,
                term: 1,
                segment: SegmentNum::new(1),
                file_offset: 0,
            },
            MockSystemEvent {
                id: 2,
                term: 3,
                segment: SegmentNum::new(1),
                file_offset: 55,
            },
            MockSystemEvent {
                id: 3,
                term: 4,
                segment: SegmentNum::new(2),
                file_offset: 77,
            },
        ];

        let (peer_1_tx, peer_1_rx) = ::engine::connection_handler::create_connection_control_channels();
        let mut controller_state = MockControllerState::new()
                .with_commit_index(2)
                .with_mocked_events(mock_system_events.as_slice())
                .with_connection(ConnectionRef {
                    connection_id: peer_1_connection,
                    remote_address: peer_1.address,
                    control_sender: peer_1_tx,
                });

        let append = ReceiveAppendEntries {
            term: 6,
            prev_entry_index: 3,
            prev_entry_term: 4,
            commit_index: 3,
            events: Vec::new(),
        };
        subject.append_entries_received(peer_1_connection, append, &mut controller_state);

        assert_eq!(State::Follower, subject.state);
        assert!(subject.primary_state.is_none());
        let actual = subject.shared.read().unwrap();
        assert_eq!(Some(peer_1.clone()), actual.system_primary);
        assert_eq!(Some(peer_1.id), subject.current_primary);
        assert!(!subject.primary_status_writer.get())
    }

    #[test]
    fn append_entries_is_sent_on_tick_when_state_is_primary() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:1000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:2000")
        };
        let peer_1_connection = 1;
        let peer_2_connection = 2;
        connection_manager.stub_peer_connection(peer_1_connection, peer_1.id);
        connection_manager.stub_peer_connection(peer_2_connection, peer_2.id);

        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        subject.persistent.modify(|state| {
            state.cluster_members.insert(peer_1.clone());
            state.cluster_members.insert(peer_2.clone());
            state.current_term = 5;
        }).unwrap();
        subject.last_applied = 99;
        subject.last_applied_term = 4;
        subject.state = State::Primary;
        subject.primary_state = Some(PrimaryState::new(5));
        subject.last_heartbeat = start;

        let mock_system_events = vec![
            MockSystemEvent {
                id: 1,
                term: 1,
                segment: SegmentNum::new(1),
                file_offset: 0,
            },
            MockSystemEvent {
                id: 2,
                term: 3,
                segment: SegmentNum::new(1),
                file_offset: 55,
            },
            MockSystemEvent {
                id: 3,
                term: 4,
                segment: SegmentNum::new(2),
                file_offset: 77,
            },
        ];

        let mut controller_state = MockControllerState::new().with_commit_index(2).with_mocked_events(mock_system_events.as_slice());
        subject.send_append_entries(&mut controller_state);

        let expected = CallAppendEntries {
            current_term: 5,
            commit_index: 2,
            reader_start_position: Some(AppendEntriesStart {
                prev_entry_index: 2,
                prev_entry_term: 3,
                reader_start_offset: 77,
                reader_start_segment: SegmentNum::new(2),
            }),
        };

        connection_manager.verify_any_order(&Invocation::SendToPeer {
            peer_id: peer_1.id,
            connection_control: ConnectionControl::SendAppendEntries(expected.clone()),
        });
        connection_manager.verify_any_order(&Invocation::SendToPeer {
            peer_id: peer_2.id,
            connection_control: ConnectionControl::SendAppendEntries(expected.clone()),
        });
    }

    #[test]
    fn new_election_is_started_on_tick_when_current_election_goes_beyond_timeout() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:1000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:2000")
        };
        let peer_1_connection = 1;
        let peer_2_connection = 2;
        connection_manager.stub_peer_connection(peer_1_connection, peer_1.id);
        connection_manager.stub_peer_connection(peer_2_connection, peer_2.id);

        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        subject.persistent.modify(|state| {
            let this_id = state.this_instance_id;
            state.voted_for = Some(this_id);
            state.cluster_members.insert(peer_1.clone());
            state.cluster_members.insert(peer_2.clone());
            state.current_term = 5;
        }).unwrap();
        subject.last_applied = 99;
        subject.last_applied_term = 4;
        subject.state = State::Voted;
        subject.last_heartbeat = start;

        let mut controller_state = MockControllerState::new();
        subject.tick(t_sec(start, 1), &mut controller_state);

        connection_manager.verify_in_order(&Invocation::EstablishConnections);
        connection_manager.verify_in_order(&Invocation::BroadcastToPeers {
            connection_control: ConnectionControl::SendRequestVote(CallRequestVote {
                term: 6,
                candidate_id: subject.persistent.this_instance_id,
                last_log_index: 99,
                last_log_term: 4,
            })
        });

        assert_eq!(6, subject.persistent.current_term);
        assert_eq!(Some(subject.persistent.this_instance_id), subject.persistent.voted_for);
        assert_eq!(State::Voted, subject.state);
    }

    #[test]
    fn vote_response_is_ignored_when_it_is_from_an_unknown_peer() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:1000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:2000")
        };
        let peer_1_connection = 1;
        let peer_2_connection = 2;
        let unknown_connection = 3;
        connection_manager.stub_peer_connection(peer_1_connection, peer_1.id);
        connection_manager.stub_peer_connection(peer_2_connection, peer_2.id);

        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        subject.persistent.modify(|state| {
            let this_id = state.this_instance_id;
            state.voted_for = Some(this_id);
            state.cluster_members.insert(peer_1.clone());
            state.cluster_members.insert(peer_2.clone());
            state.current_term = 5;
        }).unwrap();
        subject.state = State::Voted;
        subject.last_heartbeat = start;

        assert!(subject.votes_received.is_empty());

        let mut mock_controller = MockControllerState::new();
        subject.vote_response_received(t_millis(start, 4), unknown_connection, VoteResponse{
            term: 5,
            granted: true,
        }, &mut mock_controller);
        assert!(subject.votes_received.is_empty());
    }

    #[test]
    fn election_is_aborted_when_vote_response_contains_term_greater_than_current() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:1000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:2000")
        };
        let peer_3 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:3000")
        };
        let peer_4 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:4000")
        };
        let peer_1_connection = 1;
        let peer_2_connection = 2;
        let peer_3_connection = 3;
        let peer_4_connection = 4;
        connection_manager.stub_peer_connection(peer_1_connection, peer_1.id);
        connection_manager.stub_peer_connection(peer_2_connection, peer_2.id);
        connection_manager.stub_peer_connection(peer_3_connection, peer_3.id);
        connection_manager.stub_peer_connection(peer_4_connection, peer_4.id);
        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        subject.persistent.modify(|state| {
            let this_id = state.this_instance_id;
            state.cluster_members.insert(peer_1.clone());
            state.cluster_members.insert(peer_2.clone());
            state.cluster_members.insert(peer_3.clone());
            state.cluster_members.insert(peer_4.clone());
            state.current_term = 5;
        }).unwrap();
        subject.state = State::Voted;
        subject.votes_received.insert(peer_1.id); // simulate one vote having been received already

        let mut controller = MockControllerState::new();
        subject.vote_response_received(t_millis(start, 5), peer_2_connection, VoteResponse {
            term: 7,
            granted: false,
        }, &mut controller);
        assert!(subject.votes_received.is_empty());
        assert_eq!(7, subject.persistent.current_term);
        assert_eq!(State::Follower, subject.state);
    }

    #[test]
    fn vote_response_is_not_counted_when_granted_is_false() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:1000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:2000")
        };
        let peer_1_connection = 1;
        let peer_2_connection = 2;
        connection_manager.stub_peer_connection(peer_1_connection, peer_1.id);
        connection_manager.stub_peer_connection(peer_2_connection, peer_2.id);

        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        subject.persistent.modify(|state| {
            let this_id = state.this_instance_id;
            state.voted_for = Some(this_id);
            state.cluster_members.insert(peer_1.clone());
            state.cluster_members.insert(peer_2.clone());
            state.current_term = 5;
        }).unwrap();
        subject.state = State::Voted;
        subject.last_heartbeat = start;

        assert!(subject.votes_received.is_empty());
        let mut mock_controller = MockControllerState::new();
        subject.vote_response_received(t_millis(start, 4), peer_1_connection, VoteResponse{
            term: 7,
            granted: false
        }, &mut mock_controller);
        assert!(subject.votes_received.is_empty());
        assert_eq!(7, subject.persistent.current_term);
    }

    #[test]
    fn primary_status_is_set_after_a_majority_of_votes_are_granted_before_the_election_timeout() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:1000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:2000")
        };
        let peer_3 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:3000")
        };
        let peer_4 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:4000")
        };
        let peer_1_connection = 1;
        let peer_2_connection = 2;
        let peer_3_connection = 3;
        let peer_4_connection = 4;
        connection_manager.stub_peer_connection(peer_1_connection, peer_1.id);
        connection_manager.stub_peer_connection(peer_2_connection, peer_2.id);
        connection_manager.stub_peer_connection(peer_3_connection, peer_3.id);
        connection_manager.stub_peer_connection(peer_4_connection, peer_4.id);
        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        subject.persistent.modify(|state| {
            let this_id = state.this_instance_id;
            state.cluster_members.insert(peer_1.clone());
            state.cluster_members.insert(peer_2.clone());
            state.cluster_members.insert(peer_3.clone());
            state.cluster_members.insert(peer_4.clone());
            state.current_term = 5;
        }).unwrap();
        subject.state = State::Follower;
        subject.last_applied_term = 5;
        subject.last_applied = 9;

        let mut controller_state = MockControllerState::new();
        let election_start = t_sec(start, 1);
        subject.tick(election_start, &mut controller_state);
        connection_manager.verify_in_order(&Invocation::EstablishConnections);
        connection_manager.verify_in_order(&Invocation::BroadcastToPeers {
            connection_control: ConnectionControl::SendRequestVote(CallRequestVote {
                term: 6,
                candidate_id: subject.persistent.this_instance_id,
                last_log_index: subject.last_applied,
                last_log_term: subject.last_applied_term,
            }),
        });

        subject.vote_response_received(t_millis(election_start, 3), peer_1_connection, VoteResponse {
            term: 6, granted: true
        }, &mut controller_state);
        assert_eq!(State::Voted, subject.state);
        subject.vote_response_received(t_millis(election_start, 3), peer_2_connection, VoteResponse {
            term: 6, granted: true
        }, &mut controller_state);
        assert_eq!(State::Primary, subject.state);
        assert!(subject.primary_status_writer.get());
        let shared = subject.shared.read().unwrap();
        assert!(shared.this_instance_is_primary());
        let subject_id = subject.persistent.this_instance_id;
        assert_eq!(Some(subject_id), subject.current_primary);
    }

    #[test]
    fn vote_is_granted_when_candidate_term_and_log_are_more_up_to_date() {
        vote_test(|subject, request, candidate, peer_2| {
            subject.last_applied_term = request.last_log_term - 1;
            subject.last_applied = request.last_log_index - 2;
            subject.persistent.modify(|state| {
                state.current_term = request.term - 1;
                state.cluster_members.insert(candidate.clone());
                state.cluster_members.insert(peer_2.clone());
            }).unwrap();

            VoteExpectation {
                term: request.term,
                persistent_voted_for: Some(candidate.id),
                granted: true
            }
        });
    }

    #[test]
    fn vote_is_denied_when_candidate_term_is_less_than_current_term() {
        vote_test(|subject, request, candidate, peer_2| {
            subject.last_applied_term = request.last_log_term;
            subject.last_applied = request.last_log_index;
            subject.persistent.modify(|state| {
                state.current_term = request.term + 1; // my current term is greater
                state.cluster_members.insert(candidate.clone());
                state.cluster_members.insert(peer_2.clone());
            }).unwrap();

            VoteExpectation {
                term: request.term + 1,
                persistent_voted_for: None,
                granted: false
            }
        });
    }

    #[test]
    fn vote_is_denied_when_candidate_log_term_is_out_of_date() {
        vote_test(|subject, request, candidate, peer_2| {
            subject.last_applied_term = request.last_log_term + 1;
            // unless we've really screwed something up, the last_log_index should never be the same if the last_log_term is different.
            // We're doing it this way in the test just to document the behavior in this case
            subject.last_applied = request.last_log_index;
            subject.persistent.modify(|state| {
                state.current_term = request.term;
                state.cluster_members.insert(candidate.clone());
                state.cluster_members.insert(peer_2.clone());
            }).unwrap();

            VoteExpectation {
                term: request.term,
                persistent_voted_for: None,
                granted: false
            }
        });
    }

    #[test]
    fn vote_is_denied_when_candidate_is_not_a_known_peer() {
        vote_test(|subject, request, candidate, peer_2| {
            subject.last_applied_term = request.last_log_term;
            subject.last_applied = request.last_log_index;
            subject.persistent.modify(|state| {
                state.current_term = request.term - 1;
                // peer_1 is not a known member
                state.cluster_members.insert(peer_2.clone());
            }).unwrap();

            VoteExpectation {
                term: request.term - 1,
                persistent_voted_for: None,
                granted: false
            }
        });
    }

    #[test]
    fn vote_is_denied_when_candidate_log_is_out_of_date() {
        vote_test(|subject, request, candidate, peer_2| {
            subject.last_applied_term = request.last_log_term;
            subject.last_applied = request.last_log_index + 1;
            subject.persistent.modify(|state| {
                state.voted_for = None;
                state.current_term = request.term;
                state.cluster_members.insert(candidate.clone());
                state.cluster_members.insert(peer_2.clone());
            }).unwrap();

            VoteExpectation {
                term: request.term,
                persistent_voted_for: None,
                granted: false,
            }
        });
    }

    #[test]
    fn vote_is_denied_when_a_vote_was_already_cast_for_another_member_this_term() {
        vote_test(|subject, request, candidate, peer_2| {
            subject.last_applied_term = request.last_log_term;
            subject.last_applied = request.last_log_index;
            subject.persistent.modify(|state| {
                state.voted_for = Some(peer_2.id); // already voted for peer 2
                state.current_term = request.term;
                state.cluster_members.insert(candidate.clone());
                state.cluster_members.insert(peer_2.clone());
            }).unwrap();

            VoteExpectation {
                term: request.term,
                persistent_voted_for: Some(peer_2.id),
                granted: false,
            }
        });
    }

    #[test]
    fn vote_is_granted_when_no_other_vote_was_granted_and_candidate_log_exactly_matches() {
        vote_test(|subject, request, candidate, peer_2| {
            subject.last_applied_term = request.last_log_term;
            subject.last_applied = request.last_log_index;
            subject.persistent.modify(|state| {
                state.current_term = request.term - 1;
                state.cluster_members.insert(candidate.clone());
                state.cluster_members.insert(peer_2.clone());
            }).unwrap();

            VoteExpectation {
                term: request.term,
                persistent_voted_for: Some(candidate.id),
                granted: true
            }
        });
    }

    #[test]
    fn vote_is_granted_when_voted_for_is_already_populated_but_candidate_term_is_greater() {
        vote_test(|subject, request, candidate, peer_2| {
            subject.last_applied_term = request.last_log_term;
            subject.last_applied = request.last_log_index;
            subject.persistent.modify(|state| {
                state.current_term = request.term - 1;
                state.voted_for = Some(state.this_instance_id);
                state.cluster_members.insert(candidate.clone());
                state.cluster_members.insert(peer_2.clone());
            }).unwrap();

            VoteExpectation {
                term: request.term,
                persistent_voted_for: Some(candidate.id),
                granted: true
            }
        });
    }

    #[test]
    fn cluster_manager_starts_new_election_after_timeout_elapses() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:3000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:3000")
        };
        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());
        subject.state = State::Follower;
        subject.last_applied_term = 7;
        subject.last_applied = 9;
        subject.persistent.modify(|state| {
            state.current_term = 7;
        }).unwrap();

        let mut controller_state = MockControllerState::new();
        subject.tick(t_sec(start, 1), &mut controller_state);
        connection_manager.verify_in_order(&Invocation::EstablishConnections);

        let this_id = subject.persistent.this_instance_id;
        assert_eq!(Some(this_id), subject.persistent.voted_for);
        assert_eq!(8, subject.persistent.current_term);

        connection_manager.verify_in_order(&Invocation::BroadcastToPeers {
            connection_control: ConnectionControl::SendRequestVote(CallRequestVote {
                term: 8,
                candidate_id: this_id,
                last_log_index: 9,
                last_log_term: 7,
            })
        });
    }

    #[test]
    fn cluster_manager_moves_to_follower_state_once_peer_announce_is_received_with_known_primary() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:3000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:3000")
        };
        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());

        assert_eq!(State::EstablishConnections, subject.state);
        let mut controller_state = MockControllerState::new();
        subject.tick(t_sec(start, 1), &mut controller_state);
        assert_eq!(State::DeterminePrimary, subject.state);
        connection_manager.verify_in_order(&Invocation::EstablishConnections);

        let conn_id = 5;
        let upgrade = PeerUpgrade {
            peer: peer_1.clone(),
            system_primary: Some(peer_2.clone()),
            cluster_members: vec![peer_2.clone()],
        };
        let (connection, _) = mock_connection_ref(conn_id, peer_1.address);
        controller_state.add_connection(connection.clone());

        subject.peer_connection_established(upgrade, conn_id, &controller_state);
        connection_manager.verify_in_order(&Invocation::PeerConnectionEstablished {
            peer: peer_1.clone(),
            success_connection: connection,
        });

        assert_eq!(State::Follower, subject.state);
        let actual_primary: Option<SocketAddr> = {
            subject.system_partition_primary_address.read().unwrap().as_ref().cloned()
        };
        assert_eq!(Some(peer_2.address), actual_primary);
    }

    #[test]
    fn cluster_manager_moves_to_follower_state_once_all_unknown_peer_connections_have_failed() {
        let start = Instant::now();

        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1_addr = addr("111.222.0.1:3000");
        let peer_2_addr = addr("111.222.0.2:3000");
        let mut subject = create_cluster_manager(vec![peer_1_addr, peer_2_addr], temp_dir.path(), connection_manager.boxed_ref());

        assert_eq!(State::EstablishConnections, subject.state);
        let mut controller_state = MockControllerState::new();
        subject.tick(t_sec(start, 1), &mut controller_state);
        connection_manager.verify_in_order(&Invocation::EstablishConnections);
        assert_eq!(State::DeterminePrimary, subject.state);

        subject.outgoing_connection_failed(1, peer_1_addr);
        connection_manager.verify_in_order(&Invocation::OutgoingConnectionFailed {
            connection_id: 1,
            addr: peer_1_addr,
        });
        subject.outgoing_connection_failed(2, peer_2_addr);
        connection_manager.verify_in_order(&Invocation::OutgoingConnectionFailed {
            connection_id: 2,
            addr: peer_2_addr,
        });
        assert_eq!(State::Follower, subject.state);
    }

    #[test]
    fn shared_state_this_instance_is_primary_returns_false_when_system_primary_does_not_match() {
        let this_id = FloInstanceId::generate_new();
        let this_addr = addr("127.0.0.1:3000");
        let subject = SharedClusterState {
            this_instance_id: this_id,
            this_address: Some(this_addr),
            system_primary: Some(Peer {id: FloInstanceId::generate_new(), address: this_addr}),
            peers: HashSet::new(),
        };
        assert!(!subject.this_instance_is_primary());
    }

    #[test]
    fn shared_state_this_instance_is_primary_returns_false_when_system_primary_is_none() {
        let this_id = FloInstanceId::generate_new();
        let this_addr = addr("127.0.0.1:3000");
        let subject = SharedClusterState {
            this_instance_id: this_id,
            this_address: Some(this_addr),
            system_primary: None,
            peers: HashSet::new(),
        };
        assert!(!subject.this_instance_is_primary());
    }

    #[test]
    fn shared_state_this_instance_is_primary_returns_true_when_this_instance_id_matches_primary() {
        let this_id = FloInstanceId::generate_new();
        let this_addr = addr("127.0.0.1:3000");
        let subject = SharedClusterState {
            this_instance_id: this_id,
            this_address: Some(this_addr),
            system_primary: Some(Peer {id: this_id, address: this_addr}),
            peers: HashSet::new(),
        };
        assert!(subject.this_instance_is_primary());
    }

    fn this_instance_addr() -> SocketAddr {
        addr("123.1.2.3:4567")
    }

    fn create_cluster_manager(starting_peers: Vec<SocketAddr>, temp_dir: &Path, conn_manager: Box<PeerConnectionManager>) -> ClusterManager {
        let temp_file = temp_dir.join("cluster_state");
        let file_state = FilePersistedState::initialize(temp_file).expect("failed to init persistent state");

        let shared = file_state.initialize_shared_state(Some(this_instance_addr()));

        ClusterManager::new(150,
                            starting_peers,
                            this_instance_addr(),
                            file_state,
                            Arc::new(RwLock::new(shared)),
                            AtomicBoolWriter::with_value(false),
                            Arc::new(RwLock::new(None)),
                            conn_manager)
    }

    fn t_sec(start: Instant, seconds: u64) -> Instant {
        start + Duration::from_secs(seconds)
    }

    fn t_millis(start: Instant, millis: u64) -> Instant {
        start + Duration::from_millis(millis)
    }

    struct VoteExpectation {
        term: Term,
        persistent_voted_for: Option<FloInstanceId>,
        granted: bool,
    }

    fn vote_test<F>(setup_fun: F) where F: Fn(&mut ClusterManager, &CallRequestVote, Peer, Peer) -> VoteExpectation {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let connection_manager = MockPeerConnectionManager::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:3000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:3000")
        };
        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), connection_manager.boxed_ref());
        subject.state = State::Follower;

        let request_vote = CallRequestVote {
            term: 8,
            candidate_id: peer_1.id,
            last_log_index: 9,
            last_log_term: 7,
        };
        let expectation = setup_fun(&mut subject, &request_vote, peer_1.clone(), peer_2.clone());

        subject.request_vote_received(678, request_vote);

        assert_eq!(expectation.persistent_voted_for, subject.persistent.voted_for);
        assert_eq!(expectation.term, subject.persistent.current_term);

        connection_manager.verify_in_order(&Invocation::SendToPeer {
            peer_id: peer_1.id,
            connection_control: ConnectionControl::SendVoteResponse(VoteResponse {
                term: expectation.term,
                granted: expectation.granted,
            })
        });
    }
}

/// Used when the server is running in standalone mode
#[derive(Debug)]
pub struct NoOpConsensusProcessor;

// TODO: reasonable and consistent behavior for calls into NoOpConsensusProcessor that should never actually happen
impl ConsensusProcessor for NoOpConsensusProcessor {
    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, controller_state: &ControllerState) {
    }
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr) {
        panic!("invalid operation for a NoOpConsensusProcessor. This should not happen");
    }
    fn request_vote_received(&mut self, from: ConnectionId, request: CallRequestVote) {
        panic!("invalid operation for a NoOpConsensusProcessor. This should not happen");
    }
    fn tick(&mut self, now: Instant, controller_state: &mut ControllerState) {
    }
    fn is_primary(&self) -> bool {
        true
    }
    fn vote_response_received(&mut self, now: Instant, from: ConnectionId, response: VoteResponse, controller: &mut ControllerState) {
        panic!("invalid operation for a NoOpConsensusProcessor. This should not happen");
    }
    fn append_entries_received(&mut self, connection_id: ConnectionId, append: ReceiveAppendEntries, controller_state: &mut ControllerState) {
        panic!("invalid operation for a NoOpConsensusProcessor. This should not happen");
    }

    fn append_entries_response_received(&mut self, connection_id: ConnectionId, response: AppendEntriesResponse, controller_state: &mut ControllerState) {
        panic!("invalid operation for a NoOpConsensusProcessor. This should not happen");
    }
    fn connection_closed(&mut self, connection_id: ConnectionId) {

    }
    fn send_append_entries(&mut self, _controller_state: &mut ControllerState) {
        // do nothing
    }
    fn get_current_term(&self) -> Term {
        0
    }
}
