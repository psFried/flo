mod client_map;
mod cluster;

use self::cluster::ClusterState;
use self::client_map::ClientMap;
use engine::api::{ClientConnect, ConnectionId, ProducerManagerMessage, ConsumerManagerMessage, ReceivedMessage};
use engine::event_store::EventWriter;
use event::{ActorId, OwnedFloEvent, EventCounter, FloEventId, VersionVector};
use protocol::{ProtocolMessage, EventAck, ProduceEvent, ClusterMember};

use std::sync::mpsc::Sender;
use std::time::{Instant, Duration};
use std::net::SocketAddr;
use std::cmp::max;

use futures::sync::mpsc::UnboundedSender;

pub fn tick_duration() -> Duration {
    Duration::from_secs(2)
}

pub struct ProducerManager<S: EventWriter> {
    actor_id: ActorId,
    server_port: u16,
    event_store: S,
    highest_event_id: EventCounter,
    version_vec: VersionVector,
    consumer_manager_channel: Sender<ConsumerManagerMessage>,
    clients: ClientMap,
    cluster_state: ClusterState,
    cluster_connect_sender: UnboundedSender<SocketAddr>,
}

impl <S: EventWriter> ProducerManager<S> {
    pub fn new(storage: S,
               consumer_manager_channel: Sender<ConsumerManagerMessage>,
               actor_id: ActorId,
               my_port: u16,
               my_version_vec: VersionVector,
               peer_addresses: Vec<SocketAddr>,
               cluster_connect_sender: UnboundedSender<SocketAddr>) -> ProducerManager<S> {
        let highest_event_id = my_version_vec.max().event_counter;
        ProducerManager {
            actor_id: actor_id,
            server_port: my_port,
            event_store: storage,
            highest_event_id: highest_event_id,
            version_vec: my_version_vec,
            consumer_manager_channel: consumer_manager_channel,
            clients: ClientMap::new(),
            cluster_state: ClusterState::new(peer_addresses),
            cluster_connect_sender: cluster_connect_sender,
        }
    }

    pub fn process(&mut self, message: ProducerManagerMessage) -> Result<(), String> {
        match message {
            ProducerManagerMessage::Connect(connect) => {
                self.on_connect(connect)
            }
            ProducerManagerMessage::Disconnect(connection_id, address) => {
                debug!("removing producer: {} at address: {}", connection_id, address);
                self.clients.remove(connection_id);
                self.cluster_state.connection_closed(connection_id);
                Ok(())
            }
            ProducerManagerMessage::OutgoingConnectFailure(address) => {
                self.cluster_state.connect_failed(address);
                Ok(())
            }
            ProducerManagerMessage::Tick => {
                self.on_tick()
            }
            ProducerManagerMessage::Receive(message) => {
                self.process_received_message(message)
            }
        }
    }

    fn on_connect(&mut self, connect: ClientConnect) -> Result<(), String> {
        info!("Added new connection from: {} as connection_id: {}", connect.client_addr, connect.connection_id);
        let address = connect.client_addr;
        let connection_id = connect.connection_id;
        self.clients.add(connect);

        if self.cluster_state.is_disconnected_peer(&address) {
            self.on_peer_connect(connection_id, address)
        } else {
            Ok(())
        }
    }

    fn get_my_cluster_state_message(&self) -> ::protocol::ClusterState {
        let cluster_members = {
            let ProducerManager {ref cluster_state, ..} = *self;

            let mut members: Vec<ClusterMember> = cluster_state.connected_peers.values().filter(|peer| {
                peer.actor_id.is_some()
            }).map(|peer| {
                ClusterMember {
                    addr: peer.address,
                    actor_id: peer.actor_id.unwrap(), // safe unwrap because of the filter above
                    connected: true,
                }
            }).collect();

            for disconnected_peer in cluster_state.disconnected_peers.values() {
                if disconnected_peer.actor_id.is_some() {
                    let member = ClusterMember {
                        addr: disconnected_peer.address,
                        actor_id: disconnected_peer.actor_id.unwrap(), // safe unwrap due to check above
                        connected: false,
                    };
                    members.push(member);
                }
            }
            members
        };

        ::protocol::ClusterState {
            actor_id: self.actor_id,
            actor_port: self.server_port,
            version_vector: self.version_vec.snapshot(),
            other_members: cluster_members,
        }
    }

    fn on_peer_connect(&mut self, connection_id: ConnectionId, address: SocketAddr) -> Result<(), String> {
        self.cluster_state.peer_connected(address, connection_id);
        let state_message = self.get_my_cluster_state_message();
        self.clients.send(connection_id, ProtocolMessage::PeerAnnounce(state_message))
    }

    fn process_received_message(&mut self, ReceivedMessage{sender, message, ..}: ReceivedMessage) -> Result<(), String> {
        trace!("Received from connection_id: {} message: {:?}", sender, message);
        match message {
            ProtocolMessage::ProduceEvent(produce) => {
                self.new_produce_event(sender, produce)
            }
            ProtocolMessage::PeerAnnounce(cluster_state) => {
                let result = self.on_peer_announce(sender, cluster_state);
                self.cluster_state.log_state();
                result
            }
            ProtocolMessage::PeerUpdate(cluster_state) => {
                self.process_peer_cluster_state(sender, &cluster_state);
                self.cluster_state.log_state();
                Ok(())
            }
            ProtocolMessage::ReceiveEvent(event) => {
                if !self.version_vec.contains(event.id) {
                    self.persist_event(sender, 0, event)
                } else {
                    trace!("Skipping event: {} received from connection_id: {} because it's already persisted", event.id, sender);
                    Ok(())
                }
            }
            ProtocolMessage::AwaitingEvents => {
                info!("Replication is caught up for peer connection_id: {}", sender);
                Ok(())
            }
            other @ _ => {
                error!("Unhandled message: {:?}", other);
//                Err(format!("Unhandled message: {:?}", other))
                Ok(())
            }
        }
    }

    fn on_tick(&mut self) -> Result<(), String> {
        let disconnected_peers = self.cluster_state.attempt_connections(Instant::now());
        if !disconnected_peers.is_empty() {
            for peer_address in disconnected_peers {
                self.cluster_connect_sender.send(peer_address).map_err(|err| {
                    format!("Failed to send message to cluster connect sender: {}", err)
                })?; // Early return if this fails
            }
        }
        Ok(())
    }

    fn on_peer_announce(&mut self, connection_id: ConnectionId, peer_cluster_state: ::protocol::ClusterState) -> Result<(), String> {
        info!("Upgrading to peer connection_id: {}, peer_cluster_state: {:?}", connection_id, peer_cluster_state);
        self.process_peer_cluster_state(connection_id, &peer_cluster_state);

        let my_state = self.get_my_cluster_state_message();
        let ::protocol::ClusterState {actor_id, version_vector, ..} = peer_cluster_state;

        self.clients.send(connection_id, ProtocolMessage::PeerUpdate(my_state)).map_err(|err| {
            format!("Error sending response to PeerAnnounce to actor_id: {}, connection_id: {}", actor_id, connection_id)
        })
    }

    fn process_peer_cluster_state(&mut self, connection_id: ConnectionId, peer_state: &::protocol::ClusterState) {
        let rectified_address = self.rectify_peer_address(connection_id, peer_state);
        info!("Received PeerUpdate from connection_id: {}, remote address: {}, peer_state: {:?}", connection_id, rectified_address, peer_state);

        self.cluster_state.peer_message_received(rectified_address, connection_id, peer_state.actor_id);

        for member in peer_state.other_members.iter() {
            if member.actor_id != self.actor_id {
                self.cluster_state.add_peer_address(member.addr);
            }
        }
    }

    /// Returns the address that this server instance should use for connecting to the peer server identified by the given
    /// `ConnectionId`, from which we received the given `ClusterState`.
    ///
    /// We have the remote address of the peer from when we accepted the connection, but the port from that address is not going to be the
    /// same as the port that the peer is listening on. The only way to know which port the peer server is listening on
    /// is for them to tell us.
    /// The situation for the ip address is similar, but reversed. When we accept the connection, we know exactly the ip
    /// address that we should try to connect to for that peer. The peer server may _not_ know which address other should
    /// use to connect to it, though.
    ///
    /// We assume that the given `connection_id` will
    /// exist in the client map since we are processing a message that is from that client. This function will panic if
    /// that's not the case.
    fn rectify_peer_address(&self, connection_id: ConnectionId, peer_state: &::protocol::ClusterState) -> SocketAddr {
        /*
        */
        // Safe unwrap since this is the client that just sent us the message
        let mut client_remote_address = self.clients.get_client_address(connection_id).unwrap();
        debug!("Rectifying peer address for connection_id: {} from remote connection address: {} and setting port from message: {}",
        connection_id, client_remote_address, peer_state.actor_port);
        client_remote_address.set_port(peer_state.actor_port);
        client_remote_address
    }

    fn persist_event(&mut self, connection_id: ConnectionId, op_id: u32, event: OwnedFloEvent) -> Result<(), String> {
        let event_id = event.id;

        self.event_store.store(&event).map_err(|err| {
            format!("Error storing event: {:?}", err)
        }).and_then(|()| {
            self.highest_event_id = max(self.highest_event_id + 1, event_id.event_counter);
            debug!("Stored event, new highest_event_id: {}", self.highest_event_id);
            self.version_vec.update(event_id)
        }).and_then(|()| {
            let event_ack = ProtocolMessage::AckEvent(EventAck {
                op_id: op_id,
                event_id: event_id,
            });

            self.clients.send(connection_id, event_ack).map_err(|err| {
                format!("Error sending event ack to client: {:?}", err)
            }).and_then(|()| {
                self.consumer_manager_channel.send(ConsumerManagerMessage::EventPersisted(connection_id, event)).map_err(|err| {
                    format!("Error sending event ack to consumer manager: {:?}", err)
                })
            })
        })
    }

    fn new_produce_event(&mut self, connection_id: ConnectionId, ProduceEvent {op_id, namespace, parent_id, data}: ProduceEvent) -> Result<(), String> {

        let event_id = FloEventId::new(self.actor_id, self.highest_event_id + 1);
        let owned_event = OwnedFloEvent {
            id: event_id,
            timestamp: ::event::time::now(),
            namespace: namespace,
            parent_id: parent_id,
            data: data,
        };

        self.persist_event(connection_id, op_id, owned_event)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use engine::api::*;
    use protocol::{self, ServerMessage};
    use event::{FloEvent, OwnedFloEvent, ActorId, VersionVector};
    use engine::event_store::EventWriter;

    use std::sync::mpsc::{channel, Receiver};
    use std::time::Duration;
    use futures::sync::mpsc::{UnboundedReceiver, unbounded};
    use futures::{Async, Stream};

    const SUBJECT_ACTOR_ID: ActorId = 1;
    const SUBJECT_PORT: u16 = 2222;

    macro_rules! version_vec {
        ($([$actor:expr,$counter:expr]),*) => {
            {
                let mut vv = VersionVector::new();
                $( vv.update(FloEventId::new($actor, $counter)).unwrap(); )*
                vv
            }
        }
    }

    #[test]
    fn when_client_produces_event_it_is_written_and_consumer_manager_is_notified() {
        let (mut subject, mut consumer_manager) = setup();
        let client_connection_id  = 7;
        let (_client, mut client_receiver) = client_connects(client_connection_id, &mut subject);

        client_produces_event(client_connection_id, &mut subject, &mut client_receiver, &mut consumer_manager);
    }

    #[test]
    fn when_client_produces_event_then_version_vec_is_updated() {
        let (mut subject, mut consumer_manager) = setup();
        let client_connection_id  = 7;
        let (_client, mut client_receiver) = client_connects(client_connection_id, &mut subject);

        let start_counter = subject.version_vec.get(SUBJECT_ACTOR_ID);

        client_produces_event(client_connection_id, &mut subject, &mut client_receiver, &mut consumer_manager);

        let end_event_counter = subject.version_vec.get(SUBJECT_ACTOR_ID);
        assert_eq!(start_counter + 1, end_event_counter);
    }

    #[test]
    fn when_peer_sends_event_delta_then_events_are_persisted_and_version_vector_is_updated() {
        let peer_actor_id = 2;
        let subject_versions = version_vec!([SUBJECT_ACTOR_ID,5], [peer_actor_id,4]);
        let (mut subject, mut consumer_manager) = setup_with_version_map(subject_versions.clone());

        let client_connection_id  = 7;
        let (_client, mut client_receiver) = client_connects(client_connection_id, &mut subject);

        let peer_versions = vec![id(1, 3), id(peer_actor_id,7)];
        client_upgrades_to_peer(client_connection_id, peer_actor_id, peer_versions, subject_versions.clone(), &mut subject, &mut client_receiver, &mut consumer_manager);
        let event = OwnedFloEvent {
            id: FloEventId::new(peer_actor_id, 5),
            timestamp: ::event::time::from_millis_since_epoch(4),
            parent_id: None,
            namespace: "/deli/pickles".to_owned(),
            data: vec![9, 8, 7, 6, 5],
        };
        subject.process(to_producer_message(client_connection_id, ProtocolMessage::ReceiveEvent(event.clone()))).expect("failed to process replicate event message");

        assert_eq!(5, subject.version_vec.get(peer_actor_id));
        assert_event_written(event, &subject.event_store);
    }

    #[test]
    fn when_client_upgrades_to_peer_then_version_vector_is_sent_to_client() {
        let subject_versions = version_vec!([1,5], [2,4], [3,3]);
        let (mut subject, mut consumer_manager) = setup_with_version_map(subject_versions.clone());
        let client_connection_id  = 7;
        let (_client, mut client_receiver) = client_connects(client_connection_id, &mut subject);

        let peer_versions = vec![id(1,3), id(2, 9), id(6, 7)];

        client_upgrades_to_peer(client_connection_id, 2, peer_versions, subject_versions, &mut subject, &mut client_receiver, &mut consumer_manager);
    }

    fn id(actor: ActorId, counter: EventCounter) -> FloEventId {
        FloEventId::new(actor, counter)
    }

    fn client_upgrades_to_peer(client_id: ConnectionId,
                               peer_actor_id: ActorId,
                               peer_versions: Vec<FloEventId>,
                               subject_versions: VersionVector,
                               subject: &mut ProducerManager<MockEventWriter>,
                               client: &mut UnboundedReceiver<ServerMessage>,
                               consumer_manager: &mut Receiver<ConsumerManagerMessage>) {

        let peer_state = protocol::ClusterState {
            actor_id: peer_actor_id,
            actor_port: 2222,
            version_vector: peer_versions.clone(),
            other_members: vec![
                ClusterMember {
                    addr: address("127.0.0.1:3333"),
                    actor_id: SUBJECT_ACTOR_ID,
                    connected: false,
                }
            ],
        };
        let input = to_producer_message(client_id, ProtocolMessage::PeerAnnounce(peer_state));
        subject.process(input).expect("failed to process peer announce");

        assert_client_message_sent(client, |msg| {
            match msg {
                ServerMessage::Other(ProtocolMessage::PeerUpdate(cluster_state)) => {
                    assert_eq!(SUBJECT_ACTOR_ID, cluster_state.actor_id);
                    let result = VersionVector::from_vec(cluster_state.version_vector).unwrap();
                    assert_eq!(subject_versions, result);
                }
                other @ _ => {
                    panic!("expected PeerUpdate, got: {:?}", other);
                }
            }
        });
    }

    fn client_produces_event(client_id: ConnectionId,
                             subject: &mut ProducerManager<MockEventWriter>,
                             client: &mut UnboundedReceiver<ServerMessage>,
                             consumer_manager: &mut Receiver<ConsumerManagerMessage>) {

        let namespace = "/the/namespace";
        let parent_id = Some(FloEventId::new(3, 4));
        let event_data = vec![1, 2, 4, 8];
        let op_id = 1234;
        let event = ProtocolMessage::ProduceEvent(ProduceEvent {
            op_id: op_id,
            parent_id: parent_id,
            namespace: namespace.to_owned(),
            data: event_data.clone(),
        });


        subject.process(to_producer_message(client_id, event)).expect("Failed to process produce event");

        assert_client_message_sent(client, |msg| {
            match msg {
                ServerMessage::Other(ProtocolMessage::AckEvent(ack)) => {
                    assert_eq!(ack.op_id, op_id);
                },
                other @ _ => {
                    panic!("Expected event ack, got: {:?}", other)
                }
            }
        });

        assert_consumer_manager_message_sent(consumer_manager, |msg| {
            match msg {
                ConsumerManagerMessage::EventPersisted(conn, event) => {
                    assert_eq!(client_id, conn);
                    assert_eq!(namespace, &event.namespace);
                    assert_eq!(parent_id, event.parent_id);
                    assert_eq!(event_data, event.data);
                }
                other @ _ => panic!("Expected EventPersisted, got: {:?}", other)
            }
        });
    }

    fn address(addr_str: &str) -> SocketAddr {
        addr_str.parse().unwrap()
    }

    fn to_producer_message(connection_id: ConnectionId, proto_msg: ProtocolMessage) -> ProducerManagerMessage {
        ProducerManagerMessage::Receive(ReceivedMessage::received_now(connection_id, proto_msg))
    }

    fn assert_event_written(event: OwnedFloEvent, writer: &MockEventWriter) {
        assert_eq!(Some(&event), writer.stored.last());
    }

    fn assert_consumer_manager_message_sent<F>(receiver: &mut Receiver<ConsumerManagerMessage>, fun: F) where F: Fn(ConsumerManagerMessage) {
        let msg = receiver.recv_timeout(timeout()).expect("Failed to receive consumer manager message");
        fun(msg);
    }

    fn assert_client_message_sent<F>(receiver: &mut UnboundedReceiver<ServerMessage>, fun: F) where F: Fn(ServerMessage) {
        match receiver.poll().expect("failed to receive message") {
            Async::Ready(Some(message)) => fun(message),
            _ => {
                panic!("Expected to receive a message, got none")
            }
        }
    }

    fn timeout() -> Duration {
        Duration::from_millis(1)
    }

    struct MockEventWriter {
        stored: Vec<OwnedFloEvent>,
    }

    impl EventWriter for MockEventWriter {
        type Error = ();

        fn store<E: FloEvent>(&mut self, event: &E) -> Result<(), Self::Error> {
            self.stored.push(event.to_owned());
            Ok(())
        }
    }

    fn client_connects(id: ConnectionId, subject: &mut ProducerManager<MockEventWriter>) -> (ClientConnect, UnboundedReceiver<ServerMessage>) {
        use std::net::{SocketAddrV4, Ipv4Addr, SocketAddr};
        let (tx, rx) = unbounded();
        let connect = ClientConnect {
            connection_id: id,
            client_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 4444)),
            message_sender: tx,
        };
        subject.process(ProducerManagerMessage::Connect(connect.clone())).expect("failed to process client connect");
        (connect, rx)
    }

    fn setup() -> (ProducerManager<MockEventWriter>, Receiver<ConsumerManagerMessage>) {
        setup_with_version_map(VersionVector::new())
    }

    fn setup_with_version_map(map: VersionVector) -> (ProducerManager<MockEventWriter>, Receiver<ConsumerManagerMessage>) {

        let writer = MockEventWriter {
            stored: Vec::new()
        };
        let (tx, rx) = channel();
        let (cluster_sender, _rx) = ::futures::sync::mpsc::unbounded();
        let subject = ProducerManager::new(writer, tx, SUBJECT_ACTOR_ID, SUBJECT_PORT, map, Vec::new(), cluster_sender);

        (subject, rx)
    }
}


