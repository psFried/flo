
use server::engine::api::{ProduceEvent, ConsumerMessage, ProducerMessage};
use server::engine::client_map::ClientMap;
use event_store::EventWriter;
use flo_event::{ActorId, OwnedFloEvent, EventCounter, FloEventId};
use protocol::{ServerMessage, EventAck};

use std::sync::mpsc::Sender;

pub struct ProducerManager<S: EventWriter> {
    actor_id: ActorId,
    event_store: S,
    highest_event_id: EventCounter,
    consumer_manager_channel: Sender<ConsumerMessage>,
    clients: ClientMap,
}

impl <S: EventWriter> ProducerManager<S> {
    pub fn new(storage: S, consumer_manager_channel: Sender<ConsumerMessage>, actor_id: ActorId, highest_event_id: EventCounter) -> ProducerManager<S> {
        ProducerManager {
            actor_id: actor_id,
            event_store: storage,
            highest_event_id: highest_event_id,
            consumer_manager_channel: consumer_manager_channel,
            clients: ClientMap::new(),
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
            msg @ _ => Err(format!("No ProducerManager handling for client message: {:?}", msg))
        }
    }

    fn produce_event(&mut self, event: ProduceEvent) -> Result<(), String> {
        let ProduceEvent{namespace, connection_id, op_id, event_data} = event;
        let producer_id = connection_id;
        let op_id = op_id;
        let event_id = FloEventId::new(self.actor_id, self.highest_event_id + 1);
        let owned_event = OwnedFloEvent {
            id: event_id,
            namespace: namespace,
            data: event_data,
        };

        self.event_store.store(&owned_event).map_err(|err| {
            format!("Error storing event: {:?}", err)
        }).and_then(|()| {
            self.highest_event_id += 1;
            debug!("Stored event, new highest_event_id: {}", self.highest_event_id);

            let event_ack = ServerMessage::EventPersisted(EventAck {
                op_id: op_id,
                event_id: event_id,
            });
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


