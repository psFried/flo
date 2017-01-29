
use server::engine::api::{ProduceEvent, ConsumerMessage, ProducerMessage};
use server::engine::client_map::ClientMap;
use event_store::EventWriter;
use flo_event::{ActorId, OwnedFloEvent, EventCounter, FloEventId};
use protocol::{ServerMessage, ProtocolMessage, EventAck};
use server::metrics::ProducerMetrics;

use std::sync::mpsc::Sender;
use std::time::{Instant, SystemTime};

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
            parent_id: event.parent_id,
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


