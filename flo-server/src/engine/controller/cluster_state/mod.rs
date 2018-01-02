pub mod persistent;
mod peer_connections;

use std::io;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use std::time::{Instant, Duration};
use std::path::Path;
use std::collections::{HashMap, HashSet};

use event::{EventCounter, ActorId};
use engine::{ConnectionId, EngineRef};
use engine::connection_handler::ConnectionControl;
use engine::controller::{CallRequestVote, VoteResponse};
use protocol::{FloInstanceId, Term};
use atomics::{AtomicBoolWriter, AtomicBoolReader};
use super::{ClusterOptions, ConnectionRef, Peer, PeerUpgrade};
use super::peer_connection::{PeerSystemConnection, OutgoingConnectionCreator, OutgoingConnectionCreatorImpl};
use self::peer_connections::{PeerConnectionManager, PeerConnections};

pub use self::persistent::{FilePersistedState, PersistentClusterState};

/// This is a placeholder for a somewhat better error handling when updating the persistent cluster state fails.
/// This situation is extremely problematic, since it may be possible to cast two different votes in the same term, if for
/// instance, changes to the `voted_for` field cannot be persisted. For now, we're just going to have the controller panic
/// whenever there's an IO error when persisting cluster state changes. Thankfully, this error should be fairly rare, since
/// the file that's used to persist state changes is opened during initialization.
static STATE_UPDATE_FAILED: &'static str = "failed to persist changes to cluster state! Panicking, since data consistency cannot be guaranteed under these circumstances";


pub trait ConsensusProcessor: Send {
    fn tick(&mut self, now: Instant, all_connections: &mut HashMap<ConnectionId, ConnectionRef>);
    fn is_primary(&self) -> bool;
    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, all_connections: &HashMap<ConnectionId, ConnectionRef>);
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr);
    fn request_vote_received(&mut self, from: ConnectionId, request: CallRequestVote);
    fn vote_response_received(&mut self, now: Instant, from: ConnectionId, response: VoteResponse);
}

#[derive(Debug)]
pub struct NoOpConsensusProcessor;

impl ConsensusProcessor for NoOpConsensusProcessor {
    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, all_connections: &HashMap<ConnectionId, ConnectionRef>) {
    }
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr) {
        panic!("invalid operation for a NoOpConsensusProcessor. This should not happen");
    }
    fn request_vote_received(&mut self, from: ConnectionId, request: CallRequestVote) {

    }
    fn tick(&mut self, now: Instant, all_connections: &mut HashMap<ConnectionId, ConnectionRef>) {
    }
    fn is_primary(&self) -> bool {
        true
    }
    fn vote_response_received(&mut self, now: Instant, from: ConnectionId, response: VoteResponse) {

    }
}

#[derive(Debug)]
pub struct ClusterManager {
    state: State,
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
            election_timeout: Duration::from_millis(election_timeout_millis),
            last_heartbeat: Instant::now(),
            connection_manager: peer_connection_manager,
            votes_received: HashSet::new(),
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

    fn determine_primary_from_peer_upgrade(&mut self, upgrade: PeerUpgrade) {
        let PeerUpgrade { peer_id, system_primary, .. } = upgrade;
        if let Some(primary) = system_primary {
            info!("Determined primary of {:?} at {}, as told by instance: {:?}", primary.id, primary.address, peer_id);
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
            debug!("peer announce from {:?} has unknown primary", peer_id);
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
        now - self.last_heartbeat > self.election_timeout
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
        self.transition_state(State::Voted)
    }

    fn can_grant_vote(&self, request: &CallRequestVote) -> bool {
        (self.persistent.voted_for.is_none() || self.persistent.voted_for == Some(request.candidate_id)) &&
                self.persistent.current_term <= request.term &&
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

    fn transition_to_primary(&mut self) {
        self.transition_state(State::Primary);
        let this_peer = Peer {
            id: self.persistent.this_instance_id,
            address: self.this_instance_address,
        };
        self.set_new_primary(Some(this_peer));
        self.primary_status_writer.set(true);
    }

    fn set_new_primary(&mut self, primary: Option<Peer>) {
        let mut lock = self.shared.write().unwrap();
        lock.system_primary = primary;
    }
}

impl ConsensusProcessor for ClusterManager {
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr) {
        self.connection_manager.outgoing_connection_failed(connection_id, address);
        self.connection_resolved(address);
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

        self.connection_manager.send_to_peer(candidate_id, ConnectionControl::SendVoteResponse(response));
    }

    fn vote_response_received(&mut self, now: Instant, from: ConnectionId, response: VoteResponse) {
        let peer_id = self.connection_manager.get_peer_id(from);
        if peer_id.is_none() {
            error!("Ignoring Vote Response from connection_id: {}: {:?}", from, response);
            return;
        }
        let peer_id = peer_id.unwrap();

        if response.granted {
            if self.count_vote_response(peer_id) {
                self.transition_to_primary();
            }
        } else {
            let response_term = response.term;
            debug!("VoteResponse from {:?} was not granted, response term: {}, current_term: {}", peer_id, response_term, self.persistent.current_term);
            if response_term > self.persistent.current_term {
                info!("Received response term of {}, which is greater than current term of: {}. Updating current term and clearing out {} existing votes",
                        response_term, self.persistent.current_term, self.votes_received.len());
                self.persistent.modify(|state| {
                    state.current_term = response_term;
                }).expect(STATE_UPDATE_FAILED);
                self.votes_received.clear();
                self.transition_state(State::Follower);
            }
        }
    }

    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, all_connections: &HashMap<ConnectionId, ConnectionRef>) {
        debug!("peer_connection_established: connection_id: {},  {:?}", connection_id, upgrade);
        let connection = all_connections.get(&connection_id)
                .expect("Expected connection to be in all_connections on peer_connection_established");

        self.connection_manager.peer_connection_established(upgrade.peer_id, connection);

        if State::DeterminePrimary == self.state {
            self.determine_primary_from_peer_upgrade(upgrade);
        }
        self.connection_resolved(connection.remote_address);
    }

    fn tick(&mut self, now: Instant, all_connections: &mut HashMap<ConnectionId, ConnectionRef>) {
        self.connection_manager.establish_connections(now, all_connections);

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
    use engine::connection_handler::{ConnectionControlSender, ConnectionControlReceiver, ConnectionControl};
    use engine::controller::peer_connection::{OutgoingConnectionCreator, MockOutgoingConnectionCreator};
    use test_utils::{addr, expect_future_resolved};
    use engine::controller::cluster_state::peer_connections::mock::{MockPeerConnectionManager, Invocation};
    use engine::controller::controller_messages::mock::mock_connection_ref;

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

        let mut connections = HashMap::new();
        subject.tick(t_sec(start, 1), &mut connections);

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
        subject.vote_response_received(t_millis(start, 4), unknown_connection, VoteResponse{
            term: 5,
            granted: true,
        });
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

        subject.vote_response_received(t_millis(start, 5), peer_2_connection, VoteResponse {
            term: 7,
            granted: false,
        });
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
        subject.vote_response_received(t_millis(start, 4), peer_1_connection, VoteResponse{
            term: 7,
            granted: false
        });
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

        let mut connections = HashMap::new();
        let election_start = t_sec(start, 1);
        subject.tick(election_start, &mut connections);
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
        });
        assert_eq!(State::Voted, subject.state);
        subject.vote_response_received(t_millis(election_start, 3), peer_2_connection, VoteResponse {
            term: 6, granted: true
        });
        assert_eq!(State::Primary, subject.state);
        assert!(subject.primary_status_writer.reader().get_relaxed());
        let shared = subject.shared.read().unwrap();
        assert!(shared.this_instance_is_primary());
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

        let mut connections = HashMap::new();
        subject.tick(t_sec(start, 1), &mut connections);
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
        let mut connections = HashMap::new();
        subject.tick(t_sec(start, 1), &mut connections);
        assert_eq!(State::DeterminePrimary, subject.state);
        connection_manager.verify_in_order(&Invocation::EstablishConnections);

        let conn_id = 5;
        let upgrade = PeerUpgrade {
            peer_id: peer_1.id,
            system_primary: Some(peer_2.clone()),
            cluster_members: vec![peer_2.clone()],
        };
        let (connection, _) = mock_connection_ref(conn_id, peer_1.address);
        connections.insert(conn_id, connection.clone());

        subject.peer_connection_established(upgrade, conn_id, &connections);
        connection_manager.verify_in_order(&Invocation::PeerConnectionEstablished {
            peer_id: peer_1.id,
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
        let mut connections = HashMap::new();
        subject.tick(t_sec(start, 1), &mut connections);
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
