use event::ActorId;
use engine::api::ConnectionId;

use futures::sync::mpsc::UnboundedSender;

use std::net::SocketAddr;
use std::collections::HashMap;
use std::time::{Instant, Duration};


#[derive(Debug, PartialEq)]
struct ConnectedPeer {
    address: SocketAddr,
    connection_id: ConnectionId,
    connection_start_time: Instant,
    actor_id: Option<ActorId>,
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

    fn set_actor_id(&mut self, actor_id: ActorId) -> Result<(), String> {
        if self.actor_id.is_some() {
            Err(format!("Cannot change to actor_id: {} for connection_id: {} because actor_id is already: {:?}",
                        actor_id, self.connection_id, self.actor_id))
        } else {
            info!("Peer connection established on connection_id: {} as actor_id: {} at address: {}",
                    self.connection_id, actor_id, self.address);
            self.actor_id = Some(actor_id);
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq)]
struct DisconnectedPeer {
    address: SocketAddr,
    actor_id: Option<ActorId>,
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
}


pub struct ClusterState {
    connected_peers: HashMap<ConnectionId, ConnectedPeer>,
    disconnected_peers: HashMap<SocketAddr, DisconnectedPeer>,
}

impl ClusterState {
    fn new(peer_addresses: Vec<SocketAddr>) -> ClusterState {
        let new_peers = peer_addresses.into_iter().map(|addr| (addr.clone(), DisconnectedPeer::new(addr))).collect();
        ClusterState {
            connected_peers: HashMap::new(),
            disconnected_peers: new_peers,
        }
    }

    pub fn peer_message_received(&mut self, connection_id: ConnectionId, actor_id: ActorId) -> Result<(), String> {
        self.connected_peers.get_mut(&connection_id).ok_or_else(|| {
            format!("ClusterState::peer_message_received called with connection_id: {}, actor: {}, but no connected peer exists",
                    connection_id, actor_id)
        }).and_then(|peer| {
            peer.set_actor_id(actor_id)
        })
    }

    pub fn peer_connected(&mut self, address: SocketAddr, connection_id: ConnectionId) {
        let disconnected_peer = self.disconnected_peers.remove(&address).unwrap_or_else(|| {
            DisconnectedPeer::new(address)
        });

        let connected_peer = disconnected_peer.connect(connection_id);
        self.connected_peers.insert(connection_id, connected_peer);
    }

    pub fn attempt_connections(&mut self) -> Vec<SocketAddr> {
        self.disconnected_peers.values_mut().filter(|peer| {
            peer.connection_attempt_start.is_none()
        }).map(|peer| {
            peer.connection_attempt_start = Some(Instant::now());
            peer.connection_attempts += 1;
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

    #[test]
    fn peer_message_received_returns_error_when_peer_already_had_an_actor_id_assigned() {
        let peer = localhost(5678);
        let addresses = vec![localhost(1234), peer.clone(), localhost(4321)];
        let mut subject = ClusterState::new(addresses.clone());

        let connection_id = 67;
        subject.peer_connected(peer, connection_id);
        let peer_actor_id = 89;
        subject.peer_message_received(connection_id, peer_actor_id).expect("First cal should succeed");

        let result = subject.peer_message_received(connection_id, 3);
        assert!(result.is_err());
    }

    #[test]
    fn peer_message_received_returns_error_when_peer_is_disconnected() {
        let peer = localhost(5678);
        let addresses = vec![localhost(1234), peer.clone(), localhost(4321)];
        let mut subject = ClusterState::new(addresses.clone());

        let result = subject.peer_message_received(44, 31);
        assert!(result.is_err());
    }

    #[test]
    fn peer_message_received_sets_the_actor_id_of_a_peer() {
        let peer = localhost(5678);
        let addresses = vec![localhost(1234), peer.clone(), localhost(4321)];
        let mut subject = ClusterState::new(addresses.clone());

        let connection_id = 67;
        subject.peer_connected(peer, connection_id);
        let peer_actor_id = 89;
        subject.peer_message_received(connection_id, peer_actor_id).unwrap();

        let result = subject.connected_peers.remove(&connection_id).unwrap();
        assert_eq!(Some(peer_actor_id), result.actor_id);
    }

    #[test]
    fn attempt_connections_does_not_return_addresses_that_are_in_process_of_attempting_connection() {
        let addresses = vec![localhost(1234), localhost(5678), localhost(4321)];
        let mut subject = ClusterState::new(addresses.clone());

        let _ = subject.attempt_connections();
        subject.connect_failed(localhost(1234));

        let result = subject.attempt_connections();

        assert_sets_equal(vec![localhost(1234)], result);
    }

    #[test]
    fn attempt_connections_returns_addresses_of_peers_that_are_disconnected() {
        let addresses = vec![localhost(1234), localhost(5678), localhost(4321)];
        let mut subject = ClusterState::new(addresses.clone());

        let result = subject.attempt_connections();
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
