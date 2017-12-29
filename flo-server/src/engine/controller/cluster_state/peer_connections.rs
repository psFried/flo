use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Instant, Duration};

use protocol::FloInstanceId;
use engine::ConnectionId;
use engine::controller::{ConnectionRef, Peer};
use engine::controller::peer_connection::{PeerSystemConnection, OutgoingConnectionCreator};
use engine::connection_handler::ConnectionControl;

#[derive(Debug)]
pub struct PeerConnections {
    disconnected_peers: HashMap<SocketAddr, ConnectionAttempt>,
    known_peers: HashMap<FloInstanceId, Connection>,
    active_connections: HashMap<ConnectionId, FloInstanceId>,
    outgoing_connection_creator: Box<OutgoingConnectionCreator>,
}

impl PeerConnections {
    pub fn new(starting_peer_addresses: Vec<SocketAddr>, outgoing_connection_creator: Box<OutgoingConnectionCreator>, peers: &[Peer]) -> PeerConnections {
        let known_peers = peers.iter().map(|peer| {
            let connection = Connection::new(peer.address);
            (peer.id, connection)
        }).collect::<HashMap<FloInstanceId, Connection>>();

        let mut starting_peers = starting_peer_addresses.into_iter().map(|addr| {
            (addr, ConnectionAttempt::new())
        }).collect::<HashMap<_, _>>();

        for peer in peers.iter() {
            let address = peer.address;
            if !starting_peers.contains_key(&address) {
                starting_peers.insert(address, ConnectionAttempt::new());
            }
        }

        PeerConnections {
            disconnected_peers: starting_peers,
            active_connections: HashMap::new(),
            known_peers,
            outgoing_connection_creator,
        }
    }

    pub fn establish_connections(&mut self, now: Instant, all_connections: &mut HashMap<ConnectionId, ConnectionRef>) {
        let PeerConnections {ref mut disconnected_peers, ref mut outgoing_connection_creator, ..} = *self;
        for (address, attempt) in disconnected_peers.iter_mut() {
            if attempt.should_try_connect(now) {
                debug!("Making outgoing connection attempt #{} to address: {}", attempt.attempt_count + 1, address);
                let connection_ref = outgoing_connection_creator.establish_system_connection(*address);
                send(&connection_ref, ConnectionControl::InitiateOutgoingSystemConnection);
                all_connections.insert(connection_ref.connection_id, connection_ref.clone());
                attempt.attempt_time = now;
                attempt.attempt_count += 1;
                attempt.connection = Some(connection_ref);
            }
        }
    }

    pub fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr) {
        let PeerConnections {ref mut disconnected_peers, ref mut known_peers, ref mut outgoing_connection_creator, ..} = *self;
        if let Some(attempt) = disconnected_peers.get_mut(&address) {
            // remove the connection
            if attempt.connection.take().is_some() {
                // set the attempt time to the time of the failure, since it could take quite some time to get a connection error
                attempt.attempt_time = Instant::now();
            } else {
                warn!("Outgoing connection_id: {} to addr: {} failed, but no connection attempt was in progress", connection_id, address);
            }
        } else {
            warn!("Outgoing connection_id: {} to addr: {} failed, but no outgoing connection could be found", connection_id, address);
        }
    }

    pub fn peer_connection_established(&mut self, peer_id: FloInstanceId, success_connection: &ConnectionRef) {
        let disconnected = self.disconnected_peers.remove(&success_connection.remote_address);
        let success_connection_id = success_connection.connection_id;

        info!("Successfully established connection_id: {} to peer_id: {} at address: {}",
              success_connection.connection_id, peer_id, success_connection.remote_address);

        if let Some(ConnectionAttempt {connection, attempt_count, ..}) = disconnected {
            if let Some(outgoing_connection_ref) = connection {
                // if there's an existing attempt to create a connection to this peer, verify that this is indeed
                // the same connection. if this success is from a different connection than the one we were trying to establish,
                // then we'll close the in progress attempt by dropping the connectionRef
                if outgoing_connection_ref.connection_id != success_connection_id {
                    info!("Established a new connection for peer: {:?} with connection_id: {}, so existing connection: {} will be closed",
                          peer_id, success_connection_id, outgoing_connection_ref.connection_id)
                }
            }
        }

        let connection = self.known_peers.entry(peer_id).or_insert_with(|| {
            Connection::new(success_connection.remote_address)
        });
        connection.state = PeerState::Connected(success_connection.clone());

        self.active_connections.insert(success_connection_id, peer_id);
    }

    pub fn connection_closed(&mut self, connection_id: ConnectionId) {
        let address: Option<SocketAddr> = self.active_connections.remove(&connection_id).and_then(|peer_id| {
            self.known_peers.get_mut(&peer_id).map(|connection| {
                connection.state = PeerState::ConnectionFailed;
                connection.peer_address
            })
        });

        if let Some(peer_address) = address {
            // todo: as it is, we only deal with one connection per peer at a time, so we know there won't already be an entry in disconnected_peers. will need to change that once we deal with multiple connections per peer
            self.disconnected_peers.insert(peer_address, ConnectionAttempt::new());
            info!("ConnectionClosed for connection_id: {} from peer_address: {}", connection_id, peer_address);
        } else {
            // just means that this id was not for a peer connection
            debug!("Got connection closed for connection_id: {}, but there was no active peer connection", connection_id);
        }
    }

    pub fn connection_peer_id(&self, connection_id: ConnectionId) -> Option<FloInstanceId> {
        self.active_connections.get(&connection_id).cloned()
    }

}

fn send(connection: &ConnectionRef, control: ConnectionControl) {
    let result = connection.control_sender.send(control);
    if let Err(send_err) = result {
        warn!("Error sending control to connection_id: {}, {:?}", connection.connection_id, send_err);
    }
}

#[derive(Debug)]
struct ConnectionAttempt {
    attempt_count: u32,
    attempt_time: Instant,
    connection: Option<ConnectionRef>,
}

impl ConnectionAttempt {
    fn new() -> ConnectionAttempt {
        ConnectionAttempt {
            attempt_count: 0,
            attempt_time: Instant::now(),
            connection: None,
        }
    }
    fn should_try_connect(&self, now: Instant) -> bool {
        if self.connection.is_some() {
            return false;
        }

        let time_to_wait = Duration::from_secs(self.attempt_count.min(30) as u64);
        now >= self.attempt_time && (now - self.attempt_time) >= time_to_wait
    }

    fn failed(&mut self, time: Instant) {
        self.attempt_time = time;
        self.attempt_count += 1;
        self.connection = None;
    }
}

#[derive(Debug)]
struct Connection {
    peer_address: SocketAddr,
    state: PeerState,
}

impl Connection {
    fn new(peer_address: SocketAddr) -> Connection {
        Connection {
            peer_address,
            state: PeerState::Init
        }
    }
}

#[derive(Debug)]
enum PeerState {
    Init,
    ConnectionFailed,
    OutgoingConnectAttempt,
    Connected(ConnectionRef),
}


#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use engine::controller::peer_connection::MockOutgoingConnectionCreator;
    use test_utils::addr;

    #[test]
    fn outgoing_connect_success_adds_known_peer_and_connection_closed_sets_it_to_disconnected() {
        let peer_address = addr("123.45.67.8:3000");
        let peer_id = FloInstanceId::generate_new();

        let mut creator = MockOutgoingConnectionCreator::new();
        let (peer_connection, rx) = creator.stub(peer_address);

        let mut subject = PeerConnections::new(vec![peer_address], creator.boxed(), &[]);

        let mut connections = HashMap::new();
        let time = Instant::now();
        subject.establish_connections(time, &mut connections);

        subject.peer_connection_established(peer_id, &peer_connection);

        assert_eq!(Some(peer_id), subject.connection_peer_id(peer_connection.connection_id));

        subject.connection_closed(peer_connection.connection_id);

        assert!(subject.disconnected_peers.contains_key(&peer_address));
        assert_eq!(None, subject.connection_peer_id(peer_connection.connection_id));
    }

    #[test]
    fn known_peers_are_added_to_disconnected_peers_when_struct_is_initialized() {
        let peer_address = addr("123.45.67.8:3000");
        let peer = Peer {
            id: FloInstanceId::generate_new(),
            address: peer_address,
        };
        let mut creator = MockOutgoingConnectionCreator::new();
        creator.stub(peer_address);
        let subject = PeerConnections::new(Vec::new(), creator.boxed(), &[peer.clone()]);

        assert!(subject.disconnected_peers.contains_key(&peer_address));
    }

    #[test]
    fn outgoing_connect_failed_sets_status_of_starting_peer() {
        let peer_address = addr("123.45.67.8:3000");
        let new_peers = vec![peer_address];
        let mut creator = MockOutgoingConnectionCreator::new();
        creator.stub(peer_address);
        let mut subject = PeerConnections::new(new_peers, creator.boxed(), &[]);

        let mut connections = HashMap::new();
        let time = Instant::now();
        subject.establish_connections(time, &mut connections);

        assert_eq!(1, connections.len());
        {
            let attempt = subject.disconnected_peers.get(&peer_address).unwrap();
            assert_eq!(time, attempt.attempt_time);
            assert_eq!(1, attempt.attempt_count);
            assert!(attempt.connection.is_some());
        }

        subject.outgoing_connection_failed(1, peer_address);
        let attempt = subject.disconnected_peers.get(&peer_address).unwrap();
        assert!(attempt.attempt_time > time);
        assert_eq!(1, attempt.attempt_count);
        assert!(attempt.connection.is_none());
    }

    #[test]
    fn connection_attempt_should_try_returns_true_when_last_attempt_was_long_enough_in_the_past() {
        let attempt = ConnectionAttempt {
            attempt_time: Instant::now() - Duration::from_secs(30),
            attempt_count: 999,
            connection: None,
        };
        assert!(attempt.should_try_connect(Instant::now()));
    }

    #[test]
    fn connection_attempt_should_try_returns_false_when_last_attempt_was_to_recent() {
        let start = Instant::now();
        let attempt = ConnectionAttempt {
            attempt_time: start - Duration::from_millis(750),
            attempt_count: 1,
            connection: None,
        };
        assert!(!attempt.should_try_connect(start));
    }

    #[test]
    fn connection_attempt_should_try_returns_true_when_attempts_is_0() {
        let instant = Instant::now();
        let attempt = ConnectionAttempt {
            attempt_time: instant,
            attempt_count: 0,
            connection: None,
        };
        assert!(attempt.should_try_connect(instant));
    }

    #[test]
    fn connection_attempt_should_try_returns_false_when_an_attempt_is_in_progress() {
        let (tx, _rx) = ::engine::connection_handler::create_connection_control_channels();
        let conn = ConnectionRef {
            connection_id: 0,
            remote_address: addr("127.0.0.1:3456"),
            control_sender: tx,
        };
        let attempt = ConnectionAttempt {
            attempt_count: 1,
            attempt_time: Instant::now() - Duration::from_millis(5000),
            connection: Some(conn),
        };
        assert!(!attempt.should_try_connect(Instant::now()));
    }
}
