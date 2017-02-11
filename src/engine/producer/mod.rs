mod client_map;

use self::client_map::ClientMap;
use engine::api::{ProduceEvent, ConsumerMessage, ProducerMessage, VersionMap};
use engine::event_store::EventWriter;
use flo_event::{ActorId, OwnedFloEvent, EventCounter, FloEventId};
use protocol::{ServerMessage, ProtocolMessage, EventAck};
use server::metrics::ProducerMetrics;

use std::sync::mpsc::Sender;
use std::time::Instant;

pub struct ProducerManager<S: EventWriter> {
    actor_id: ActorId,
    event_store: S,
    highest_event_id: EventCounter,
    consumer_manager_channel: Sender<ConsumerMessage>,
    clients: ClientMap,
    metrics: ProducerMetrics,
}

impl <S: EventWriter> ProducerManager<S> {
    pub fn new(storage: S, consumer_manager_channel: Sender<ConsumerMessage>, actor_id: ActorId, highest_event_id: EventCounter) -> ProducerManager<S> {
        ProducerManager {
            actor_id: actor_id,
            event_store: storage,
            highest_event_id: highest_event_id,
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
            msg @ _ => Err(format!("No ProducerManager handling for client message: {:?}", msg))
        }
    }

    fn produce_event(&mut self, event: ProduceEvent) -> Result<(), String> {
        let ProduceEvent{namespace, connection_id, parent_id, op_id, event_data, message_recv_start} = event;

        let produce_start = Instant::now();
        let time_in_channel = produce_start.duration_since(message_recv_start);


        let producer_id = connection_id;
        let op_id = op_id;
        let event_id = FloEventId::new(self.actor_id, self.highest_event_id + 1);
        let owned_event = OwnedFloEvent {
            id: event_id,
            timestamp: ::time::now(),
            namespace: namespace,
            parent_id: parent_id,
            data: event_data,
        };

        self.event_store.store(&owned_event).map_err(|err| {
            format!("Error storing event: {:?}", err)
        }).and_then(|()| {
            self.highest_event_id += 1;
            debug!("Stored event, new highest_event_id: {}", self.highest_event_id);

            let storage_time = produce_start.elapsed();
            self.metrics.event_persisted(event_id, time_in_channel, storage_time);

            let event_ack = ServerMessage::Other(ProtocolMessage::AckEvent(EventAck {
                op_id: op_id,
                event_id: event_id,
            }));
            self.clients.send(producer_id, event_ack).map_err(|err| {
                format!("Error sending event ack to client: {:?}", err)
            }).and_then(|()| {
                self.consumer_manager_channel.send(ConsumerMessage::EventPersisted(producer_id, owned_event)).map_err(|err| {
                    format!("Error sending event ack to consumer manager: {:?}", err)
                })
            })
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use engine::api::*;
    use protocol::*;
    use flo_event::{FloEvent, OwnedFloEvent, ActorId, EventCounter};
    use engine::event_store::EventWriter;
    use std::sync::mpsc::{channel, Sender, Receiver};
    use std::time::{Instant, Duration};
    use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded};
    use futures::{Async, Stream};

    const SUBJECT_ACTOR_ID: ActorId = 1;


    #[test]
    fn when_client_produces_event_it_is_written_and_consumer_manager_is_notified() {
        let (mut subject, mut consumer_manager) = setup();
        let client_connection_id  = 7;
        let (client, mut client_receiver) = client_connects(client_connection_id, &mut subject);

        client_produces_event(client_connection_id, &mut subject, &mut client_receiver, &mut consumer_manager);
    }

    #[test]
    fn when_client_upgrades_to_peer_then_peer_announce_is_sent_to_consumer_manager() {
        let (mut subject, mut consumer_manager) = setup();
        let client_connection_id  = 7;
        let (client, mut client_receiver) = client_connects(client_connection_id, &mut subject);

        panic!("need to finish this test");
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

        assert_message_received(client, |msg| {
            match msg {
                ServerMessage::Other(ProtocolMessage::AckEvent(ack)) => {
                    assert_eq!(ack.op_id, op_id);
                },
                other @ _ => {
                    panic!("Expected event ack, got: {:?}", other)
                }
            }
        });

        assert_consumer_manager_received(consumer_manager, |msg| {
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

    fn assert_consumer_manager_received<F>(receiver: &mut Receiver<ConsumerMessage>, fun: F) where F: Fn(ConsumerMessage) {
        let msg = receiver.recv_timeout(timeout()).expect("Failed to receive consumer manager message");
        fun(msg);
    }

    fn assert_message_received<F>(receiver: &mut UnboundedReceiver<ServerMessage>, fun: F) where F: Fn(ServerMessage) {
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
        subject.process(ProducerMessage::ClientConnect(connect.clone()));
        (connect, rx)
    }

    fn setup() -> (ProducerManager<MockEventWriter>, Receiver<ConsumerMessage>) {
        let writer = MockEventWriter {
            stored: Vec::new()
        };
        let (tx, rx) = channel();
        let subject = ProducerManager::new(writer, tx, SUBJECT_ACTOR_ID, 0);
        (subject, rx)
    }

    fn setup_with_version_map(map: VersionMap) -> (ProducerManager<MockEventWriter>, Receiver<ConsumerMessage>) {

        let writer = MockEventWriter {
            stored: Vec::new()
        };
        let (tx, rx) = channel();
        let mut subject = ProducerManager::new(writer, tx, SUBJECT_ACTOR_ID, 0);

        (subject, rx)
    }
}


