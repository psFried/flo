use engine::connection_handler::ConnectionControl;
use engine::ConnectionId;
use engine::controller::{ConnectionRef, ControllerState, Peer};
use engine::controller::peer_connection::OutgoingConnectionCreator;
use protocol::FloInstanceId;
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

pub trait PeerConnectionManager: Send + Debug + 'static {
    fn establish_connections(&mut self, now: Instant, controller_state: &mut ControllerState);
    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, addr: SocketAddr);
    fn connection_closed(&mut self, connection_id: ConnectionId);
    fn peer_connection_established(&mut self, peer: Peer, success_connection: &ConnectionRef);

    fn broadcast_to_peers(&mut self, connection_control: ConnectionControl);
    fn send_to_peer(&mut self, peer_id: FloInstanceId, connection_control: ConnectionControl);
    fn get_peer_id(&mut self, connection_id: ConnectionId) -> Option<FloInstanceId>;
}

#[derive(Debug)]
pub struct PeerConnections {
    disconnected_peers: HashMap<SocketAddr, ConnectionAttempt>,
    known_peers: HashMap<FloInstanceId, Connection>,
    active_connections: HashMap<ConnectionId, FloInstanceId>,
    outgoing_connection_creator: Box<OutgoingConnectionCreator>,
}

impl PeerConnections {
    pub fn new(starting_peer_addresses: Vec<SocketAddr>, outgoing_connection_creator: Box<OutgoingConnectionCreator>, peers: Vec<Peer>) -> PeerConnections {
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

    fn close_connection(&mut self, connection_id: ConnectionId, attempt_reconnect: bool) {
        let address: Option<SocketAddr> = self.active_connections.remove(&connection_id).and_then(|peer_id| {
            self.known_peers.get_mut(&peer_id).and_then(|connection| {
                // only set the connection state back to ConnectionFailed if the connection here is the same as the one we are closing
                if connection.get_connection_id() == Some(connection_id) {
                    connection.state = PeerState::ConnectionFailed;
                    Some(connection.peer_address)
                } else {
                    // The connection in `known_peers` has a different connection id. This means that we won't want to
                    // add the address to `disconected_peers`, even if `attempt_reconnect` is true, because it would result in
                    // multiple connections open for the same peer
                    None
                }
            })
        });

        if attempt_reconnect {
            if let Some(peer_address) = address {
                // todo: as it is, we only deal with one connection per peer at a time, so we know there won't already be an entry in disconnected_peers. will need to change that once we deal with multiple connections per peer
                self.disconnected_peers.insert(peer_address, ConnectionAttempt::new());
                info!("ConnectionClosed for connection_id: {} from peer_address: {}", connection_id, peer_address);
            } else {
                // just means that this id was not for a peer connection
                debug!("Got connection closed for connection_id: {}, but there was no active peer connection", connection_id);
            }
        }
    }
}


impl PeerConnectionManager for PeerConnections {

    fn establish_connections(&mut self, now: Instant, controller_state: &mut ControllerState) {
        let PeerConnections {ref mut disconnected_peers, ref mut outgoing_connection_creator, ..} = *self;
        for (address, attempt) in disconnected_peers.iter_mut() {
            if attempt.should_try_connect(now) {
                debug!("Making outgoing connection attempt #{} to address: {}", attempt.attempt_count + 1, address);
                let connection_ref = outgoing_connection_creator.establish_system_connection(*address);
                let _ = send(&connection_ref, ConnectionControl::InitiateOutgoingSystemConnection); // ignore result since it can't really fail
                controller_state.add_connection(connection_ref.clone());
                attempt.attempt_time = now;
                attempt.attempt_count += 1;
                attempt.connection = Some(connection_ref);
            }
        }
    }

    fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, address: SocketAddr) {
        if let Some(attempt) = self.disconnected_peers.get_mut(&address) {
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

    fn peer_connection_established(&mut self, peer: Peer, success_connection: &ConnectionRef) {
        let disconnected = self.disconnected_peers.remove(&peer.address);
        let success_connection_id = success_connection.connection_id;

        info!("Successfully established connection_id: {} to peer: {:?} at address: {}",
              success_connection.connection_id, peer, success_connection.remote_address);

        if let Some(ConnectionAttempt {connection, ..}) = disconnected {
            if let Some(outgoing_connection_ref) = connection {
                // if there's an existing attempt to create a connection to this peer, verify that this is indeed
                // the same connection. if this success is from a different connection than the one we were trying to establish,
                // then we'll close the in progress attempt by dropping the connectionRef
                if outgoing_connection_ref.connection_id != success_connection_id {
                    info!("Established a new connection for peer: {:?} with connection_id: {}, so existing connection: {} will be closed",
                          peer, success_connection_id, outgoing_connection_ref.connection_id)
                }
            }
        }

        let prev_connection = {
            let connection = self.known_peers.entry(peer.id).or_insert_with(|| {
                // TODO: We should instead just refuse connections from unknown peers for now
                Connection::new(peer.address)
            });
            connection.connection_established(success_connection.clone())
        };
        if let Some(to_close) = prev_connection {
            debug!("A second connection was established for peer: {:?} with connection_id: {}, connection_id: {} will be closed", peer, success_connection.connection_id, to_close);
            self.close_connection(to_close, false);
        }

        self.active_connections.insert(success_connection_id, peer.id);
    }

    fn connection_closed(&mut self, connection_id: ConnectionId) {
        self.close_connection(connection_id, true);
    }

    fn broadcast_to_peers(&mut self, control: ConnectionControl) {
        debug!("Broadcasting {:?}", control);
        let mut errors = Vec::new();
        for (peer, connection) in self.known_peers.iter() {
            match connection.state {
                PeerState::Connected(ref connection_ref) => {
                    let result = send(connection_ref, control.clone());
                    if result.is_err() {
                        info!("Error broadcasting connection control to handler for connection_id: {}, peer: {:?}, closing connection", connection_ref.connection_id, peer);
                        errors.push(connection_ref.connection_id);
                    }
                }
                ref other @ _ => {
                    trace!("Skipping connection to {:?} because it is in state: {:?}", peer, other);
                }
            }
        }

        for bad_connection in errors {
            self.connection_closed(bad_connection);
        }
    }

    fn send_to_peer(&mut self, peer_id: FloInstanceId, control: ConnectionControl) {
        let disconnect = if let Some(connection) = self.known_peers.get_mut(&peer_id) {
            match &mut connection.state {
                &mut PeerState::Connected(ref mut connection_ref) => {
                    let result = connection_ref.control_sender.unbounded_send(control);
                    if result.is_err() {
                        info!("Failed to send ConnectionControl to peer connection handler for {:?}, connection_id: {}, closing connection", peer_id, connection_ref.connection_id);
                        Some(connection_ref.connection_id)
                    } else {
                        None
                    }
                }
                other @ _ => {
                    debug!("Cannot send control to peer_id: {:?} because it is in state: {:?}, dropping message: {:?}", peer_id, other, control);
                    None
                }
            }
        } else {
            debug!("Cannot send control to unknown peer_id: {:?}, dropping message: {:?}", peer_id, control);
            None
        };

        if let Some(id) = disconnect {
            self.connection_closed(id);
        }
    }

    fn get_peer_id(&mut self, connection_id: ConnectionId) -> Option<FloInstanceId> {
        self.active_connections.get(&connection_id).cloned()
    }
}

fn send(connection: &ConnectionRef, control: ConnectionControl) -> Result<(), ()> {
    debug!("Sending to connection_id: {}, control: {:?}", connection.connection_id, control);
    connection.control_sender.unbounded_send(control).map_err(|send_err| {
        info!("Error sending control to connection_id: {}, {:?}", connection.connection_id, send_err);
    })
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

    fn get_connection_id(&self) -> Option<ConnectionId> {
        match self.state {
            PeerState::Connected(ref conn) => Some(conn.connection_id),
            _ => None
        }
    }

    fn connection_established(&mut self, connection_ref: ConnectionRef) -> Option<ConnectionId> {
        let prev_conn = match self.state {
            PeerState::Connected(ref existing) => Some(existing.connection_id),
            _ => None
        };
        self.state = PeerState::Connected(connection_ref);
        prev_conn
    }
}

#[derive(Debug)]
enum PeerState {
    Init,
    ConnectionFailed,
    Connected(ConnectionRef),
}


#[cfg(test)]
mod test {
    use engine::connection_handler::{CallAppendEntries, ConnectionControlReceiver};
    use engine::controller::CallRequestVote;
    use engine::controller::mock::MockControllerState;
    use engine::controller::peer_connection::MockOutgoingConnectionCreator;
    use protocol::flo_instance_id;
    use std::time::Duration;
    use super::*;
    use test_utils::{addr, expect_future_resolved};

    fn assert_control_sent(rx: ConnectionControlReceiver, expected: &ConnectionControl) -> ConnectionControlReceiver {
        use futures::Stream;
        let (message, stream) = expect_future_resolved(rx.into_future()).expect("failed to receive control message");
        let message = message.expect("did not receive any control message");
        assert_eq!(&message, expected);
        stream
    }

    fn subject_with_connected_peers(peers: &[Peer], creator: MockOutgoingConnectionCreator) -> (PeerConnections, MockControllerState) {
        let mut subject = PeerConnections::new(Vec::new(), creator.boxed(), peers.to_owned());
        let mut controller_state = MockControllerState::new();
        subject.establish_connections(Instant::now(), &mut controller_state);
        for peer in peers {
            let connection = controller_state.all_connections.values()
                    .find(|conn| conn.remote_address == peer.address)
                    .unwrap();
            subject.peer_connection_established(peer.clone(), connection);
        }
        (subject, controller_state)
    }

    #[test]
    fn send_to_peer_sends_connection_control_to_a_connected_peer() {
        let peer_id = flo_instance_id::generate_new();
        let peer = Peer {
            id: peer_id,
            address: addr("123.4.5.6:3000")
        };
        let mut creator = MockOutgoingConnectionCreator::new();
        let (_peer_conn, rx) = creator.stub(peer.address);

        let (mut subject, _connections) = subject_with_connected_peers(&[peer], creator);
        let rx = assert_control_sent(rx, &ConnectionControl::InitiateOutgoingSystemConnection);

        let expected = ConnectionControl::SendAppendEntries(CallAppendEntries {
            current_term: 3,
            commit_index: 5,
            reader_start_position: None,
        });
        subject.send_to_peer(peer_id, expected.clone());
        assert_control_sent(rx, &expected);
    }

    #[test]
    fn broadcast_sends_control_to_all_connected_peers() {
        let peer_1 = Peer {
            id: flo_instance_id::generate_new(),
            address: addr("123.4.5.6:3000")
        };
        let peer_2 = Peer {
            id: flo_instance_id::generate_new(),
            address: addr("123.4.5.6:4000")
        };
        let mut creator = MockOutgoingConnectionCreator::new();
        let (_peer_1_conn, rx_1) = creator.stub(peer_1.address);
        let (_peer_2_conn, rx_2) = creator.stub(peer_2.address);
        let (mut subject, _connections) = subject_with_connected_peers(&[peer_1, peer_2], creator);
        let rx_1 = assert_control_sent(rx_1, &ConnectionControl::InitiateOutgoingSystemConnection);
        let rx_2 = assert_control_sent(rx_2, &ConnectionControl::InitiateOutgoingSystemConnection);

        let candidate = flo_instance_id::generate_new();
        let expected = ConnectionControl::SendRequestVote(CallRequestVote {
            term: 7,
            candidate_id: candidate,
            last_log_index: 99,
            last_log_term: 6,
        });
        subject.broadcast_to_peers(expected.clone());

        assert_control_sent(rx_1, &expected);
        assert_control_sent(rx_2, &expected);
    }

    #[test]
    fn outgoing_connect_success_adds_known_peer_and_connection_closed_sets_it_to_disconnected() {
        let peer_address = addr("123.45.67.8:3000");
        let peer_id = flo_instance_id::generate_new();

        let mut creator = MockOutgoingConnectionCreator::new();
        let (peer_connection, _rx) = creator.stub(peer_address);

        let mut subject = PeerConnections::new(vec![peer_address], creator.boxed(), Vec::new());

        let mut controller_state = MockControllerState::new();
        let time = Instant::now();
        subject.establish_connections(time, &mut controller_state);

        let peer = Peer {
            id: peer_id,
            address: peer_address,
        };
        subject.peer_connection_established(peer, &peer_connection);

        assert_eq!(Some(peer_id), subject.get_peer_id(peer_connection.connection_id));

        subject.connection_closed(peer_connection.connection_id);

        assert!(subject.disconnected_peers.contains_key(&peer_address));
        assert_eq!(None, subject.get_peer_id(peer_connection.connection_id));
    }

    #[test]
    fn known_peers_are_added_to_disconnected_peers_when_struct_is_initialized() {
        let peer_address = addr("123.45.67.8:3000");
        let peer = Peer {
            id: flo_instance_id::generate_new(),
            address: peer_address,
        };
        let mut creator = MockOutgoingConnectionCreator::new();
        creator.stub(peer_address);
        let all_peers = vec![peer.clone()];
        let subject = PeerConnections::new(Vec::new(), creator.boxed(), all_peers);

        assert!(subject.disconnected_peers.contains_key(&peer_address));
    }

    #[test]
    fn outgoing_connect_failed_sets_status_of_starting_peer() {
        let peer_address = addr("123.45.67.8:3000");
        let new_peers = vec![peer_address];
        let mut creator = MockOutgoingConnectionCreator::new();
        creator.stub(peer_address);
        let mut subject = PeerConnections::new(new_peers, creator.boxed(), Vec::new());

        let mut controller_state = MockControllerState::new();
        let time = Instant::now();
        subject.establish_connections(time, &mut controller_state);

        assert_eq!(1, controller_state.all_connections.len());
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

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::collections::VecDeque;

    #[derive(Debug, Clone)]
    pub struct MockPeerConnectionManager {
        actual_invocations: Arc<Mutex<VecDeque<Invocation>>>,
        peer_stubs: Arc<Mutex<HashMap<ConnectionId, FloInstanceId>>>,
    }

    impl MockPeerConnectionManager {
        pub fn new() -> MockPeerConnectionManager {
            MockPeerConnectionManager {
                actual_invocations: Arc::new(Mutex::new(VecDeque::new())),
                peer_stubs: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub fn stub_peer_connection(&self, connection_id: ConnectionId, peer_id: FloInstanceId) {
            let mut lock = self.peer_stubs.lock().unwrap();
            lock.insert(connection_id, peer_id);
        }

        pub fn verify_in_order(&self, expected: &Invocation) {
            // lock will be poisoned if this panics
            let mut lock = self.actual_invocations.lock().unwrap();

            let missing_invocation_message = format!("Expected invocation: {:?}, but no calls were made on this mock", expected);
            let next_invocation = lock.pop_front().expect(&missing_invocation_message);
            if expected != &next_invocation {
                panic!("Expected: {:#?}, but actual was: {:#?}. Other invocations were: {:#?}", expected, next_invocation, ::std::ops::Deref::deref(&lock));
            }
        }

        pub fn verify_any_order(&self, expected: &Invocation) {
            // lock will be poisoned if this panics
            let mut lock = self.actual_invocations.lock().unwrap();

            let index = lock.iter().position(|actual| actual == expected);
            if let Some(idx) = index {
                lock.remove(idx);
            } else {
                panic!("Expected: {:#?} in any order, other invocations on this mock: {:#?}", expected, lock.as_slices());
            }
        }

        pub fn boxed_ref(&self) -> Box<PeerConnectionManager> {
            Box::new(self.clone())
        }

        fn push_invocation(&self, invocation: Invocation) {
            let mut lock = self.actual_invocations.lock().unwrap();
            lock.push_back(invocation);
        }
    }

    impl PeerConnectionManager for MockPeerConnectionManager {
        fn establish_connections(&mut self, _now: Instant, _controller_state: &mut ControllerState) {
            self.push_invocation(Invocation::EstablishConnections);
        }
        fn outgoing_connection_failed(&mut self, connection_id: ConnectionId, addr: SocketAddr) {
            self.push_invocation(Invocation::OutgoingConnectionFailed {connection_id, addr});
            let mut lock = self.peer_stubs.lock().unwrap();
            lock.remove(&connection_id);
        }
        fn connection_closed(&mut self, connection_id: ConnectionId) {
            self.push_invocation(Invocation::ConnectionClosed {connection_id});
            let mut lock = self.peer_stubs.lock().unwrap();
            lock.remove(&connection_id);
        }
        fn peer_connection_established(&mut self, peer: Peer, success_connection: &ConnectionRef) {
            self.stub_peer_connection(success_connection.connection_id, peer.id);
            self.push_invocation(Invocation::PeerConnectionEstablished {peer, success_connection: success_connection.clone()});
        }
        fn broadcast_to_peers(&mut self, connection_control: ConnectionControl) {
            self.push_invocation(Invocation::BroadcastToPeers {connection_control});
        }
        fn send_to_peer(&mut self, peer_id: FloInstanceId, connection_control: ConnectionControl) {
            self.push_invocation(Invocation::SendToPeer {peer_id, connection_control});
        }
        fn get_peer_id(&mut self, connection_id: ConnectionId) -> Option<FloInstanceId> {
            let lock = self.peer_stubs.lock().unwrap();
            lock.get(&connection_id).cloned()
        }
    }



    #[derive(Debug, PartialEq, Clone)]
    pub enum Invocation {
        EstablishConnections,
        OutgoingConnectionFailed{
            connection_id: ConnectionId,
            addr: SocketAddr,
        },
        ConnectionClosed{ connection_id: ConnectionId },
        PeerConnectionEstablished{peer: Peer, success_connection: ConnectionRef},
        BroadcastToPeers{connection_control: ConnectionControl},
        SendToPeer{peer_id: FloInstanceId, connection_control :ConnectionControl},
    }
}
