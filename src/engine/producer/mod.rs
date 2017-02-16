mod client_map;

use self::client_map::ClientMap;
use engine::api::{ProduceEvent, ConsumerMessage, ProducerMessage, PeerVersionMap, ConnectionId};
use engine::event_store::EventWriter;
use engine::version_vec::VersionVector;
use flo_event::{ActorId, OwnedFloEvent, EventCounter, FloEventId};
use protocol::{ProtocolMessage, EventAck};
use server::metrics::ProducerMetrics;

use std::sync::mpsc::Sender;
use std::time::Instant;
use std::cmp::max;

pub struct ProducerManager<S: EventWriter> {
    actor_id: ActorId,
    event_store: S,
    highest_event_id: EventCounter,
    version_vec: VersionVector,
    consumer_manager_channel: Sender<ConsumerMessage>,
    clients: ClientMap,
    metrics: ProducerMetrics,
}

impl <S: EventWriter> ProducerManager<S> {
    pub fn new(storage: S, consumer_manager_channel: Sender<ConsumerMessage>, actor_id: ActorId, my_version_vec: VersionVector) -> ProducerManager<S> {
        let highest_event_id = my_version_vec.get(actor_id);
        ProducerManager {
            actor_id: actor_id,
            event_store: storage,
            highest_event_id: highest_event_id,
            version_vec: my_version_vec,
            consumer_manager_channel: consumer_manager_channel,
            clients: ClientMap::new(),
            metrics: ProducerMetrics::new(),
        }
    }

    pub fn process(&mut self, client_message: ProducerMessage) -> Result<(), String> {
        match client_message {
            ProducerMessage::ClientConnect(client_connect) => {
                self.clients.add(client_connect);
                Ok(())
            }
            ProducerMessage::Produce(produce_event) => {
                self.produce_event(produce_event)
            }
            ProducerMessage::Disconnect(connection_id) => {
                debug!("removing producer: {}", connection_id);
                self.clients.remove(connection_id);
                Ok(())
            }
            ProducerMessage::PeerAnnounce(peer_version_map) => {
                self.peer_announce(peer_version_map)
            }
            ProducerMessage::ReplicateEvent(connection_id, event, message_recv_time) => {
                self.replicate_event(connection_id, event, message_recv_time)
            }
            msg @ _ => Err(format!("No ProducerManager handling for client message: {:?}", msg))
        }
    }

    fn replicate_event(&mut self, connection_id: ConnectionId, event: OwnedFloEvent, message_recv_time: Instant) -> Result<(), String> {
        let counter_for_actor = self.version_vec.get(event.id.actor);
        if event.id.event_counter > counter_for_actor {
            trace!("Replicating event: {:?} from connection: {}", event.id, connection_id);
            self.persist_event(connection_id, 0, event, message_recv_time)
        } else {
            trace!("No need to replicate event {:?} on connection: {} because the counter is less than the current one: {}",
                    event.id, connection_id, counter_for_actor);
            Ok(())
        }
    }

    fn peer_announce(&mut self, peer_version_map: PeerVersionMap) -> Result<(), String> {
        info!("Upgrading to peer connection: {:?}", peer_version_map);
        //Take a snapshot of the current version vector
        let my_version_vec = self.version_vec.snapshot();
        let message = ProtocolMessage::PeerUpdate {
            actor_id: self.actor_id,
            version_vec: my_version_vec,
        };
        self.clients.send(peer_version_map.connection_id, message).and_then(|()| {
            self.consumer_manager_channel.send(ConsumerMessage::StartPeerReplication(peer_version_map)).map_err(|send_err| {
                format!("Failed to send message to consumer manager: {:?}", send_err)
            })
        })
    }

    fn persist_event(&mut self, connection_id: ConnectionId, op_id: u32, event: OwnedFloEvent, message_recv_time: Instant) -> Result<(), String> {
        let produce_start = Instant::now();
        let time_in_channel = produce_start.duration_since(message_recv_time);
        let event_id = event.id;

        self.event_store.store(&event).map_err(|err| {
            format!("Error storing event: {:?}", err)
        }).and_then(|()| {
            self.highest_event_id = max(self.highest_event_id + 1, event_id.event_counter);
            debug!("Stored event, new highest_event_id: {}", self.highest_event_id);
            self.version_vec.update(event_id)
        }).and_then(|()| {
            let storage_time = produce_start.elapsed();
            self.metrics.event_persisted(event_id, time_in_channel, storage_time);

            let event_ack = ProtocolMessage::AckEvent(EventAck {
                op_id: op_id,
                event_id: event_id,
            });
            self.clients.send(connection_id, event_ack).map_err(|err| {
                format!("Error sending event ack to client: {:?}", err)
            }).and_then(|()| {
                self.consumer_manager_channel.send(ConsumerMessage::EventPersisted(connection_id, event)).map_err(|err| {
                    format!("Error sending event ack to consumer manager: {:?}", err)
                })
            })
        })
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

        self.persist_event(connection_id, op_id, owned_event, message_recv_start)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use engine::api::*;
    use protocol::*;
    use flo_event::{FloEvent, OwnedFloEvent, ActorId};
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
        let (client, mut client_receiver) = client_connects(client_connection_id, &mut subject);

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
        subject.process(ProducerMessage::ReplicateEvent(client_connection_id, event.clone(), Instant::now())).expect("failed to process replicate event message");

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
                               consumer_manager: &mut Receiver<ConsumerMessage>) {
        let peer_versions = PeerVersionMap {
            connection_id: client_id,
            from_actor: peer_actor_id,
            actor_versions: peer_versions,
        };

        let input = ProducerMessage::PeerAnnounce(peer_versions.clone());
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
                ConsumerMessage::StartPeerReplication(versions) => {
                    assert_eq!(peer_versions, versions);
                }
                other @ _ => panic!("Expected StartPeerReplication message, got: {:?}", other)
            }
        });
    }

    fn client_produces_event(client_id: ConnectionId,
                             subject: &mut ProducerManager<MockEventWriter>,
                             client: &mut UnboundedReceiver<ServerMessage>,
                             consumer_manager: &mut Receiver<ConsumerMessage>) {

        let namespace = "/the/namespace";
        let parent_id = Some(FloEventId::new(3, 4));
        let event_data = vec![1, 2, 4, 8];
        let op_id = 1234;
        let event = ProducerMessage::Produce(ProduceEvent{
            message_recv_start: Instant::now(),
            namespace: namespace.to_owned(),
            connection_id: client_id,
            op_id: op_id,
            parent_id: parent_id,
            event_data: event_data.clone(),
        });

        subject.process(event).expect("Failed to process produce event");

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
                ConsumerMessage::EventPersisted(conn, event) => {
                    assert_eq!(client_id, conn);
                    assert_eq!(namespace, &event.namespace);
                    assert_eq!(parent_id, event.parent_id);
                    assert_eq!(event_data, event.data);
                }
                other @ _ => panic!("Expected EventPersisted, got: {:?}", other)
            }
        });
    }

    fn assert_event_written(event: OwnedFloEvent, writer: &MockEventWriter) {
        assert_eq!(Some(&event), writer.stored.last());
    }

    fn assert_consumer_manager_message_sent<F>(receiver: &mut Receiver<ConsumerMessage>, fun: F) where F: Fn(ConsumerMessage) {
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
        subject.process(ProducerMessage::ClientConnect(connect.clone())).expect("failed to process client connect");
        (connect, rx)
    }

    fn setup() -> (ProducerManager<MockEventWriter>, Receiver<ConsumerMessage>) {
        setup_with_version_map(VersionVector::new())
    }

    fn setup_with_version_map(map: VersionVector) -> (ProducerManager<MockEventWriter>, Receiver<ConsumerMessage>) {

        let writer = MockEventWriter {
            stored: Vec::new()
        };
        let (tx, rx) = channel();
        let subject = ProducerManager::new(writer, tx, SUBJECT_ACTOR_ID, map);

        (subject, rx)
    }
}


