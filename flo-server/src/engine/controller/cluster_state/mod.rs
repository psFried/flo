mod persistent;
mod peer_connections;

use std::io;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use std::time::{Instant, Duration};
use std::path::Path;
use std::collections::{HashMap, HashSet};

use event::EventCounter;
use engine::{ConnectionId, EngineRef};
use protocol::FloInstanceId;
use atomics::{AtomicBoolWriter, AtomicBoolReader};
use super::{ClusterOptions, ConnectionRef, Peer, PeerUpgrade};
use super::peer_connection::{PeerSystemConnection, OutgoingConnectionCreator, OutgoingConnectionCreatorImpl};
use self::peer_connections::PeerConnections;

pub use self::persistent::{FilePersistedState, PersistentClusterState};

pub trait ConsensusProcessor: Send {
    fn tick(&mut self, now: Instant, all_connections: &mut HashMap<ConnectionId, ConnectionRef>);
    fn is_primary(&self) -> bool;
    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, all_connections: &HashMap<ConnectionId, ConnectionRef>);
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr);
}

#[derive(Debug)]
pub struct NoOpConsensusProcessor;

impl ConsensusProcessor for NoOpConsensusProcessor {
    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, all_connections: &HashMap<ConnectionId, ConnectionRef>) {
    }
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr) {
        panic!("invalid operation for a NoOpConsensusProcessor. This should not happen");
    }
    fn tick(&mut self, now: Instant, all_connections: &mut HashMap<ConnectionId, ConnectionRef>) {
    }
    fn is_primary(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct ClusterManager {
    state: State,
    initialization_peers: HashSet<SocketAddr>,
    election_timeout: Duration,
    last_heartbeat: Instant,
    primary_status_writer: AtomicBoolWriter,
    last_applied: EventCounter,
    persistent: FilePersistedState,
    system_partition_primary_address: SystemPrimaryAddressRef,
    shared: Arc<RwLock<SharedClusterState>>,
    connection_manager: PeerConnections,
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
           persistent: FilePersistedState,
           shared: Arc<RwLock<SharedClusterState>>,
           primary_status_writer: AtomicBoolWriter,
           system_partition_primary_address: SystemPrimaryAddressRef,
           outgoing_connection_creator: Box<OutgoingConnectionCreator>) -> ClusterManager {

        let mut initialization_peers = starting_peer_addresses.iter().cloned().collect::<HashSet<_>>();
        for peer in persistent.cluster_members.iter() {
            initialization_peers.insert(peer.address);
        }

        let peer_connections = PeerConnections::new(starting_peer_addresses, outgoing_connection_creator, &persistent.cluster_members);

        ClusterManager {
            state: State::EstablishConnections,
            election_timeout: Duration::from_millis(election_timeout_millis),
            last_heartbeat: Instant::now(),
            connection_manager: peer_connections,
            initialization_peers,
            last_applied: 0,
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
        let PeerUpgrade { peer_id, system_primary, cluster_members } = upgrade;
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
}

impl ConsensusProcessor for ClusterManager {
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr) {
        self.connection_manager.outgoing_connection_failed(connection_id, address);
        self.connection_resolved(address);
    }

    fn peer_connection_established(&mut self, upgrade: PeerUpgrade, connection_id: ConnectionId, all_connections: &HashMap<ConnectionId, ConnectionRef>) {
        let connection = all_connections.get(&connection_id)
                .expect("Expected connection to be in all_connections on peer_connection_established");

        self.connection_manager.peer_connection_established(upgrade.peer_id, connection);

        if let State::DeterminePrimary = self.state {
            self.determine_primary_from_peer_upgrade(upgrade);
            self.connection_resolved(connection.remote_address);
        }
    }

    fn tick(&mut self, now: Instant, all_connections: &mut HashMap<ConnectionId, ConnectionRef>) {
        match self.state {
            State::EstablishConnections => {
                self.connection_manager.establish_connections(now, all_connections);
                self.transition_state(State::DeterminePrimary);
            }
            _ => { }
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
    pub peers: Vec<Peer>,
}

impl SharedClusterState {
    pub fn non_cluster() -> SharedClusterState {
        SharedClusterState {
            this_instance_id: FloInstanceId::generate_new(),
            this_address: None,
            system_primary: None,
            peers: Vec::new(),
        }
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

    let ClusterOptions{ peer_addresses, event_loop_handles, election_timeout_millis, .. } = options;

    let outgoing_connection_creator = OutgoingConnectionCreatorImpl::new(event_loop_handles, engine_ref);

    let state = ClusterManager::new(election_timeout_millis,
                                    peer_addresses,
                                    persistent_state,
                                    shared_state_ref,
                                    system_primary,
                                    primary_address,
                                    Box::new(outgoing_connection_creator));
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
    use test_utils::addr;

    fn t(start: Instant, seconds: u64) -> Instant {
        start + Duration::from_secs(seconds)
    }

    #[test]
    fn cluster_manager_moves_to_follower_state_once_peer_announce_is_received_with_known_primary() {
        let start = Instant::now();
        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let mut mock_creator = MockOutgoingConnectionCreator::new();
        let peer_1 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.1:3000")
        };
        let peer_2 = Peer {
            id: FloInstanceId::generate_new(),
            address: addr("111.222.0.2:3000")
        };
        let (peer_1_connection, _) = mock_creator.stub(peer_1.address);
        let _ = mock_creator.stub(peer_2.address);
        let mut subject = create_cluster_manager(vec![peer_1.address, peer_2.address], temp_dir.path(), mock_creator.boxed());

        assert_eq!(State::EstablishConnections, subject.state);
        let mut connections = HashMap::new();
        subject.tick(t(start, 1), &mut connections);
        assert_eq!(State::DeterminePrimary, subject.state);

        let upgrade = PeerUpgrade {
            peer_id: peer_1.id,
            system_primary: Some(peer_2.clone()),
            cluster_members: vec![peer_2.clone()],
        };
        subject.peer_connection_established(upgrade, peer_1_connection.connection_id, &connections);

        assert_eq!(State::Follower, subject.state);
        let actual_primary: Option<SocketAddr> = {
            subject.system_partition_primary_address.read().unwrap().as_ref().cloned()
        };

    }

    #[test]
    fn cluster_manager_moves_to_follower_state_once_all_unknown_peer_connections_have_failed() {
        let start = Instant::now();

        let temp_dir = TempDir::new("cluster_state_test").unwrap();
        let mut mock_creator = MockOutgoingConnectionCreator::new();
        let peer_1_addr = addr("111.222.0.1:3000");
        let peer_2_addr = addr("111.222.0.2:3000");
        let _ = mock_creator.stub(peer_1_addr);
        let _ = mock_creator.stub(peer_2_addr);
        let mut subject = create_cluster_manager(vec![peer_1_addr, peer_2_addr], temp_dir.path(), mock_creator.boxed());

        assert_eq!(State::EstablishConnections, subject.state);
        let mut connections = HashMap::new();
        subject.tick(t(start, 1), &mut connections);
        assert_eq!(State::DeterminePrimary, subject.state);

        assert_eq!(2, connections.len());
        let actual = connections.values().map(|cr| cr.remote_address).collect::<HashSet<_>>();

        assert!(actual.contains(&peer_1_addr));
        assert!(actual.contains(&peer_2_addr));

        // make sure it doesn't happen again
        subject.tick(t(start, 5), &mut connections);
        assert_eq!(2, connections.len());

        subject.outgoing_connection_failed(1, peer_1_addr);
        subject.outgoing_connection_failed(2, peer_2_addr);
        assert_eq!(State::Follower, subject.state);
    }

    fn this_instance_addr() -> SocketAddr {
        addr("123.1.2.3:4567")
    }

    fn create_cluster_manager(starting_peers: Vec<SocketAddr>, temp_dir: &Path, conn_creator: Box<OutgoingConnectionCreator>) -> ClusterManager {
        let temp_file = temp_dir.join("cluster_state");
        let file_state = FilePersistedState::initialize(temp_file).expect("failed to init persistent state");

        let shared = file_state.initialize_shared_state(Some(this_instance_addr()));

        ClusterManager::new(150,
                            starting_peers,
                            file_state,
                            Arc::new(RwLock::new(shared)),
                            AtomicBoolWriter::with_value(false),
                            Arc::new(RwLock::new(None)),
                            conn_creator)
    }
}
