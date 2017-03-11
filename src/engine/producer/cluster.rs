use event::ActorId;
use engine::api::ConnectionId;
use engine::version_vec::VersionVector;

use futures::sync::mpsc::UnboundedSender;

use std::net::SocketAddr;
use std::collections::{HashMap, HashSet};
use std::time::{Instant, Duration};


#[derive(Debug, PartialEq)]
pub struct ConnectedPeer {
    pub address: SocketAddr,
    pub connection_id: ConnectionId,
    pub actor_id: Option<ActorId>,
    connection_start_time: Instant,
    last_message_received: Option<Instant>,
}

impl ConnectedPeer {
    fn new(address: SocketAddr, connection_id: ConnectionId, actor_id: Option<ActorId>, last_message_received: Option<Instant>) -> ConnectedPeer {
        ConnectedPeer {
            address: address,
            connection_id: connection_id,
            connection_start_time: Instant::now(),
            actor_id: actor_id,
            last_message_received: last_message_received,
        }
    }

    fn set_actor_id(&mut self, actor_id: ActorId) {
        if let Some(ref mut current_id) = self.actor_id {
            if *current_id != actor_id {
                warn!("Changing actor id for connection_id: {} at address: {} from {} to {}",
                        self.connection_id, self.address, current_id, actor_id);
            }
        }
        self.actor_id = Some(actor_id);
    }
}

#[derive(Debug, PartialEq)]
pub struct DisconnectedPeer {
    pub address: SocketAddr,
    pub actor_id: Option<ActorId>,
    last_message_received: Option<Instant>,
    connection_attempt_start: Option<Instant>,
    connection_attempts: u64,
    last_failure_time: Option<Instant>,
}

impl DisconnectedPeer {
    fn new(address: SocketAddr) -> DisconnectedPeer {
        DisconnectedPeer {
            address: address,
            actor_id: None,
            connection_attempt_start: None,
            last_message_received: None,
            connection_attempts: 0,
            last_failure_time: None
        }
    }

    fn connect(self, connection_id: ConnectionId) -> ConnectedPeer {
        let DisconnectedPeer{address, actor_id, last_message_received, ..} = self;
        ConnectedPeer::new(address, connection_id, actor_id, last_message_received)
    }

    fn should_attempt_connection(&self, current_time: Instant) -> bool {
        if self.connection_attempt_start.is_none() {
            self.last_failure_time.map(|last_failure| {
                let min_duration = self.get_reconnect_interval();
                current_time - last_failure >= min_duration
            }).unwrap_or(true)
        } else {
            false
        }
    }

    fn get_reconnect_interval(&self) -> Duration {
        let seconds = match self.connection_attempts {
            0 => 0,
            1...4 => 1,
            5...9 => 3,
            10...19 => 10,
            _ => 30
        };
        Duration::from_secs(seconds)
    }
}

impl From<ConnectedPeer> for DisconnectedPeer {
    fn from(ConnectedPeer {address, actor_id, connection_id, connection_start_time, last_message_received}: ConnectedPeer) -> Self {
        DisconnectedPeer {
            address: address,
            actor_id: actor_id,
            last_message_received: last_message_received,
            connection_attempt_start: None,
            connection_attempts: 0,
            last_failure_time: None,
        }
    }
}

#[derive(Debug)]
pub struct ClusterState {
    pub connected_peers: HashMap<ConnectionId, ConnectedPeer>,
    pub disconnected_peers: HashMap<SocketAddr, DisconnectedPeer>,
    all_peers: HashSet<SocketAddr>,
}

impl ClusterState {
    pub fn new(peer_addresses: Vec<SocketAddr>) -> ClusterState {
        let mut state = ClusterState {
            connected_peers: HashMap::new(),
            disconnected_peers: HashMap::new(),
            all_peers: HashSet::new(),
        };
        for addr in peer_addresses {
            state.add_peer_address(addr);
        }
        state
    }

    pub fn add_peer_address(&mut self, peer_address: SocketAddr) {
        if !self.all_peers.contains(&peer_address) {
            debug!("Discovered new peer at address: {}", peer_address);
            self.all_peers.insert(peer_address);
            self.disconnected_peers.insert(peer_address, DisconnectedPeer::new(peer_address));
        }
    }

    pub fn peer_message_received(&mut self, peer_address: SocketAddr, connection_id: ConnectionId, actor_id: ActorId) {
        if !self.connected_peers.contains_key(&connection_id) {
            self.peer_connected(peer_address, connection_id);
        }

        if let Some(ref mut peer) = self.connected_peers.get_mut(&connection_id) {
            peer.set_actor_id(actor_id);
        }
    }

    pub fn connection_closed(&mut self, connection_id: ConnectionId) {
        self.connected_peers.remove(&connection_id).map(|connected_peer| {
            self.disconnected_peers.insert(connected_peer.address, connected_peer.into());
        });
    }

    pub fn is_disconnected_peer(&self, address: &SocketAddr) -> bool {
        self.disconnected_peers.contains_key(&address)
    }

    pub fn peer_connected(&mut self, address: SocketAddr, connection_id: ConnectionId) {

        let disconnected_peer = self.disconnected_peers.remove(&address);

        debug!("peer_connected at address: {}, connection_id: {}, previous peer state: {:?}", address, connection_id, disconnected_peer);
        let connected_peer = disconnected_peer.unwrap_or_else(|| {
            DisconnectedPeer::new(address)
        }).connect(connection_id);

        self.connected_peers.insert(connection_id, connected_peer);
        self.all_peers.insert(address);
    }

    pub fn attempt_connections(&mut self, current_time: Instant) -> Vec<SocketAddr> {
        self.disconnected_peers.values_mut().filter(|peer| {
            peer.should_attempt_connection(current_time)
        }).map(|peer| {
            peer.connection_attempt_start = Some(current_time);
            peer.connection_attempts += 1;
            info!("Attempting connection to peer at: {} - attempt #{}", peer.address, peer.connection_attempts);
            peer.address.clone()
        }).collect()
    }

    pub fn connect_failed(&mut self, address: SocketAddr) {
        match self.disconnected_peers.get_mut(&address) {
            Some(ref mut peer) => {
                peer.last_failure_time = Some(Instant::now());
                peer.connection_attempt_start = None;
                warn!("Connection to peer at {} has failed", address);
            }
            None => {
                error!("Connect Failed for Address: {} but that address is not found in the disconnected peers map", address);
            }
        }
    }

    pub fn log_state(&self) {
        debug!("Current ClusterState: {:?}", self);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use event::ActorId;

    use std::net::SocketAddr;
    use std::time::{Instant, Duration};
    use std::collections::HashSet;
    use std::hash::Hash;

    fn localhost(port: u16) -> SocketAddr {
        let addr_string = format!("127.0.0.1:{}", port);
        addr_string.parse().unwrap()
    }

    fn test_should_attempt_reconnection(prev_attempts: u64, seconds_since_last: u64, expected_result: bool) {
        let time = Instant::now();
        let last_failure_time = time - Duration::from_secs(seconds_since_last);
        let subject = DisconnectedPeer{
            address: localhost(45),
            actor_id: None,
            last_message_received: None,
            connection_attempt_start: None,
            connection_attempts: prev_attempts,
            last_failure_time: Some(last_failure_time),
        };

        assert_eq!(expected_result, subject.should_attempt_connection(time),
                   "Expected result: {} after {} previous attempts and {} seconds",
                   expected_result,
                   prev_attempts,
                   seconds_since_last);
    }

    #[test]
    fn connection_closed_changes_a_connected_peer_to_a_disconnected_peer() {
        let mut subject = ClusterState::new(Vec::new());
        let connection_id = 8;
        let address = localhost(9999);
        subject.peer_connected(address, connection_id);
        subject.peer_message_received(address, connection_id, 6);

        subject.connection_closed(connection_id);
        assert!(subject.disconnected_peers.contains_key(&address));
        assert!(subject.connected_peers.is_empty());
    }

    #[test]
    fn disconnected_peer_reconnect_interval_increases_with_connection_attempts() {
        fn test(prev_attempts: u64, expected_interval_secs: u64) {
            let mut peer = DisconnectedPeer::new(localhost(7777));
            peer.connection_attempts = prev_attempts;

            assert_eq!(expected_interval_secs, peer.get_reconnect_interval().as_secs());
        }

        test(0, 0);
        test(1, 1);
        test(4, 1);
        test(5, 3);
        test(9, 3);
        test(10, 10);
        test(19, 10);
        test(20, 30);
        test(99999999, 30);
    }

    #[test]
    fn peer_should_attempt_connection_has_an_incremental_backoff() {
        // args are: previous connection attempts, seconds since last failure, expected result
        test_should_attempt_reconnection(0, 1, true);
        test_should_attempt_reconnection(1, 1, true);
        test_should_attempt_reconnection(1, 0, false);
        test_should_attempt_reconnection(3, 1, true);

        test_should_attempt_reconnection(5, 1, false);
        test_should_attempt_reconnection(5, 5, true);

        test_should_attempt_reconnection(10, 9, false);
        test_should_attempt_reconnection(10, 10, true);
        test_should_attempt_reconnection(19, 10, true);

        test_should_attempt_reconnection(20, 29, false);
        test_should_attempt_reconnection(20, 31, true);
        test_should_attempt_reconnection(99999, 31, true);
    }

    #[test]
    fn peer_should_attempt_connection_returns_true_for_first_attempt() {
        let now = Instant::now();
        let peer = DisconnectedPeer::new(localhost(3333));
        assert!(peer.should_attempt_connection(now));
    }

    #[test]
    fn peer_message_received_sets_the_actor_id_of_a_peer() {
        let peer = localhost(5678);
        let addresses = vec![localhost(1234), peer.clone(), localhost(4321)];
        let mut subject = ClusterState::new(addresses.clone());

        let connection_id = 67;
        subject.peer_connected(peer, connection_id);
        let peer_actor_id = 89;
        subject.peer_message_received(peer, connection_id, peer_actor_id);

        let result = subject.connected_peers.remove(&connection_id).unwrap();
        assert_eq!(Some(peer_actor_id), result.actor_id);
    }

    #[test]
    fn peer_message_received_adds_peer_address_to_set_of_all_addresses() {
        let mut subject = ClusterState::new(Vec::new());

        let peer_address = localhost(666);
        subject.peer_message_received(peer_address, 5, 4);

        assert!(subject.all_peers.contains(&peer_address));
    }

    #[test]
    fn attempt_connections_does_not_return_addresses_that_are_in_process_of_attempting_connection() {
        let addresses = vec![localhost(1234), localhost(5678), localhost(4321)];
        let mut subject = ClusterState::new(addresses.clone());

        let _ = subject.attempt_connections(Instant::now());
        subject.connect_failed(localhost(1234));

        let future = Instant::now() + Duration::from_secs(120);
        let result = subject.attempt_connections(future);

        assert_sets_equal(vec![localhost(1234)], result);
    }

    #[test]
    fn attempt_connections_returns_addresses_of_peers_that_are_disconnected() {
        let addresses = vec![localhost(1234), localhost(5678), localhost(4321)];
        let mut subject = ClusterState::new(addresses.clone());

        let result = subject.attempt_connections(Instant::now());
        assert_sets_equal(addresses, result);
    }

    #[test]
    fn peer_connected_creates_new_connected_peer_if_one_does_not_exist_in_map() {
        let address = localhost(1234);
        let mut subject = ClusterState::new(Vec::new());
        subject.peer_connected(address, 34);

        let result = subject.connected_peers.remove(&34).expect("expected to get peer");
        assert_eq!(address, result.address);
        assert_eq!(34, result.connection_id);
        assert!(result.last_message_received.is_none());
        assert!(result.actor_id.is_none());
    }

    #[test]
    fn peer_connected_moves_disconnected_peer_to_connected_peer() {
        let address = localhost(1234);
        let mut subject = ClusterState::new(vec![address.clone()]);
        subject.peer_connected(address, 34);

        assert!(subject.disconnected_peers.is_empty());

        let result = subject.connected_peers.remove(&34).expect("expected to get peer");
        assert_eq!(address, result.address);
        assert_eq!(34, result.connection_id);
        assert!(result.last_message_received.is_none());
        assert!(result.actor_id.is_none());

    }

    fn assert_sets_equal<T: Hash + Eq + ::std::fmt::Debug>(expected: Vec<T>, actual: Vec<T>) {
        let e = expected.into_iter().collect::<HashSet<T>>();
        let a = actual.into_iter().collect::<HashSet<T>>();
        assert_eq!(e, a);
    }
}
