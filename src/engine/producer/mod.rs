mod client_map;
mod cluster;

use self::cluster::ClusterState;
use self::client_map::ClientMap;
use engine::api::{ProduceEvent, ProducerMessage, PeerVersionMap, ConnectionId, ProducerManagerMessage, ConsumerManagerMessage, ReceivedMessage};
use engine::event_store::EventWriter;
use engine::version_vec::VersionVector;
use event::{ActorId, OwnedFloEvent, EventCounter, FloEventId};
use protocol::{ProtocolMessage, EventAck, NewProduceEvent};
use server::metrics::ProducerMetrics;

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
    event_store: S,
    highest_event_id: EventCounter,
    version_vec: VersionVector,
    consumer_manager_channel: Sender<ConsumerManagerMessage>,
    clients: ClientMap,
    metrics: ProducerMetrics,
    cluster_state: ClusterState,
    cluster_connect_sender: UnboundedSender<SocketAddr>,
}

impl <S: EventWriter> ProducerManager<S> {
    pub fn new(storage: S,
               consumer_manager_channel: Sender<ConsumerManagerMessage>,
               actor_id: ActorId,
               my_version_vec: VersionVector,
               peer_addresses: Vec<SocketAddr>,
               cluster_connect_sender: UnboundedSender<SocketAddr>) -> ProducerManager<S> {
        let highest_event_id = my_version_vec.get(actor_id);
        ProducerManager {
            actor_id: actor_id,
            event_store: storage,
            highest_event_id: highest_event_id,
            version_vec: my_version_vec,
            consumer_manager_channel: consumer_manager_channel,
            clients: ClientMap::new(),
            metrics: ProducerMetrics::new(),
            cluster_state: ClusterState::new(peer_addresses),
            cluster_connect_sender: cluster_connect_sender,
        }
    }

    pub fn process(&mut self, message: ProducerManagerMessage) -> Result<(), String> {
        match message {
            ProducerManagerMessage::Connect(connect) => {
                info!("Added new connection from: {} as connection_id: {}", connect.client_addr, connect.connection_id);
                self.clients.add(connect);
                Ok(())
            }
            ProducerManagerMessage::Disconnect(connection_id, address) => {
                debug!("removing producer: {} at address: {}", connection_id, address);
                self.clients.remove(connection_id);
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

    fn process_received_message(&mut self, ReceivedMessage{sender, recv_time, message}: ReceivedMessage) -> Result<(), String> {
        match message {
            ProtocolMessage::NewProduceEvent(produce) => {
                self.new_produce_event(sender, produce)
            }
            ProtocolMessage::PeerAnnounce(actor_id, peer_versions) => {
                self.peer_announce(sender, actor_id, peer_versions, recv_time)
            }
            ProtocolMessage::NewReceiveEvent(event) => {
                self.persist_event(sender, 0, event)
            }
            other @ _ => {
                error!("Unhandled message: {:?}", other);
                Err(format!("Unhandled message: {:?}", other))
            }
        }
    }

    fn outgoing_peer_connection(&mut self, connection_id: ConnectionId, address: SocketAddr) -> Result<(), String> {
        self.cluster_state.peer_connected(address, connection_id);
//        let announce_message = ProtocolMessage::PeerAnnounce()
        //TODO: Send outgoing message to peer to announce and request event replication
        Ok(())
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

    fn replicate_event(&mut self, connection_id: ConnectionId, event: OwnedFloEvent, message_recv_time: Instant) -> Result<(), String> {
        let counter_for_actor = self.version_vec.get(event.id.actor);
        if event.id.event_counter > counter_for_actor {
            trace!("Replicating event: {:?} from connection: {}", event.id, connection_id);
            self.persist_event(connection_id, 0, event)
        } else {
            trace!("No need to replicate event {:?} on connection: {} because the counter is less than the current one: {}",
                    event.id, connection_id, counter_for_actor);
            Ok(())
        }
    }

    fn peer_announce(&mut self, connection_id: ConnectionId, actor_id: ActorId, peer_versions: Vec<FloEventId>, message_recv_time: Instant) -> Result<(), String> {
        info!("Upgrading to peer connection_id: {}, actor_id: {}, peer_versions: {:?}", connection_id, actor_id, peer_versions);
        //Take a snapshot of the current version vector
        let my_version_vec = self.version_vec.snapshot();
        let message = ProtocolMessage::PeerUpdate {
            actor_id: self.actor_id,
            version_vec: my_version_vec,
        };
        self.clients.send(connection_id, message).and_then(|()| {
            self.consumer_manager_channel.send(ConsumerManagerMessage::StartPeerReplication(connection_id, actor_id, peer_versions)).map_err(|send_err| {
                format!("Failed to send message to consumer manager: {:?}", send_err)
            })
        })
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

    fn new_produce_event(&mut self, connection_id: ConnectionId, NewProduceEvent{op_id, namespace, parent_id, data}: NewProduceEvent) -> Result<(), String> {

        let event_id = FloEventId::new(self.actor_id, self.highest_event_id + 1);
        let owned_event = OwnedFloEvent {
            id: event_id,
            timestamp: ::time::now(),
            namespace: namespace,
            parent_id: parent_id,
            data: data,
        };

        self.persist_event(connection_id, op_id, owned_event)
    }

    fn produce_event(&mut self, event: ProduceEvent) -> Result<(), String> {
        let ProduceEvent{namespace, connection_id, parent_id, op_id, event_data, message_recv_start} = event;

        let event_id = FloEventId::new(self.actor_id, self.highest_event_id + 1);
        let owned_event = OwnedFloEvent {
            id: event_id,
            timestamp: ::time::now(),
            namespace: namespace,
            parent_id: parent_id,
            data: event_data,
        };

        self.persist_event(connection_id, op_id, owned_event)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use engine::api::*;
    use protocol::*;
    use event::{FloEvent, OwnedFloEvent, ActorId};
    use engine::event_store::EventWriter;
    use engine::version_vec::VersionVector;

    use std::sync::mpsc::{channel, Receiver};
    use std::time::{Instant, Duration};
    use futures::sync::mpsc::{UnboundedReceiver, unbounded};
    use futures::{Async, Stream};

    const SUBJECT_ACTOR_ID: ActorId = 1;

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
            timestamp: ::time::from_millis_since_epoch(4),
            parent_id: None,
            namespace: "/deli/pickles".to_owned(),
            data: vec![9, 8, 7, 6, 5],
        };
        subject.process(to_producer_message(client_connection_id, ProtocolMessage::NewReceiveEvent(event.clone()))).expect("failed to process replicate event message");

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

        let input = to_producer_message(client_id, ProtocolMessage::PeerAnnounce(peer_actor_id, peer_versions.clone()));
        subject.process(input).expect("failed to process peer announce");

        assert_client_message_sent(client, |msg| {
            match msg {
                ServerMessage::Other(ProtocolMessage::PeerUpdate {actor_id, version_vec}) => {
                    let result = VersionVector::from_vec(version_vec).unwrap();
                    assert_eq!(subject_versions, result);
                    assert_eq!(SUBJECT_ACTOR_ID, actor_id);
                }
                other @ _ => {
                    panic!("expected PeerUpdate, got: {:?}", other);
                }
            }
        });

        assert_consumer_manager_message_sent(consumer_manager, |msg| {
            match msg {
                ConsumerManagerMessage::StartPeerReplication(connection_id, actor_id, versions) => {
                    assert_eq!(peer_versions, versions);
                    assert_eq!(client_id, connection_id);
                    assert_eq!(peer_actor_id, actor_id);
                }
                other @ _ => panic!("Expected StartPeerReplication message, got: {:?}", other)
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
        let event = ProtocolMessage::NewProduceEvent(NewProduceEvent{
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
        let subject = ProducerManager::new(writer, tx, SUBJECT_ACTOR_ID, map, Vec::new(), cluster_sender);

        (subject, rx)
    }
}


