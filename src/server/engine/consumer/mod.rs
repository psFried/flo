mod client;
mod cache;

pub use self::client::{Client, ClientState, ClientSendError, ConsumingState};

use server::engine::api::{ConnectionId, ConsumerMessage, ClientConnect};
use protocol::ServerMessage;
use flo_event::{FloEvent, OwnedFloEvent, FloEventId};

use std::sync::{Arc, mpsc};
use std::thread;
use std::collections::HashMap;

use self::cache::Cache;
use server::MemoryLimit;
use event_store::EventReader;

pub struct ConsumerManager<R: EventReader + 'static> {
    event_reader: R,
    my_sender: mpsc::Sender<ConsumerMessage>,
    consumers: ConsumerMap,
    greatest_event_id: FloEventId,
    cache: Cache
}

impl <R: EventReader + 'static> ConsumerManager<R> {
    pub fn new(reader: R, sender: mpsc::Sender<ConsumerMessage>, greatest_event_id: FloEventId, max_cached_events: usize, max_cache_memory: MemoryLimit) -> Self {
        ConsumerManager {
            my_sender: sender,
            event_reader: reader,
            consumers: ConsumerMap::new(),
            greatest_event_id: greatest_event_id,
            cache: Cache::new(max_cached_events, max_cache_memory),
        }
    }

    pub fn process(&mut self, message: ConsumerMessage) -> Result<(), String> {
        trace!("Got message: {:?}", message);
        match message {
            ConsumerMessage::ClientConnect(connect) => {
                self.consumers.add(connect);
                Ok(())
            }
            ConsumerMessage::StartConsuming(connection_id, namespace, limit) => {
                self.start_consuming(connection_id, namespace, limit)
            }
            ConsumerMessage::ContinueConsuming(connection_id, _event_id, limit) => {
                unimplemented!()
            }
            ConsumerMessage::EventLoaded(connection_id, event) => {
                self.update_greatest_event(event.id);
                self.consumers.send_event(connection_id, Arc::new(event))
            }
            ConsumerMessage::EventPersisted(_connection_id, event) => {
                self.update_greatest_event(event.id);
                let event_rc = self.cache.insert(event);
                self.consumers.send_event_to_all(event_rc)
            }
            ConsumerMessage::UpdateMarker(connection_id, event_id) => {
                self.consumers.update_consumer_position(connection_id, event_id)
            }
            m @ _ => {
                error!("Got unhandled message: {:?}", m);
                panic!("Got unhandled message: {:?}", m);
            }
        }
    }

    fn update_greatest_event(&mut self, id: FloEventId) {
        if id > self.greatest_event_id {
            self.greatest_event_id = id;
        }
    }

    fn start_consuming(&mut self, connection_id: ConnectionId, namespace: String, limit: i64) -> Result<(), String> {
        let ConsumerManager{ref mut consumers, ref mut event_reader, ref mut my_sender, ref cache, ..} = *self;

        consumers.get_mut(connection_id).map(|mut client| {
            let start_id = client.get_current_position();

            if start_id < cache.last_evicted_id() {
                // need to read event from disk since it isn't in the cache
                let event_iter = event_reader.load_range(start_id, limit as usize);
                let event_sender = my_sender.clone();
                client.start_consuming(ConsumingState::forward_from_file(start_id, limit as u64));

                thread::spawn(move || {
                    let mut sent_events = 0;
                    let mut last_sent_id = FloEventId::zero();
                    for event in event_iter {
                        match event {
                            Ok(owned_event) => {
                                trace!("Reader thread sending event: {:?} to consumer manager", owned_event.id());
                                //TODO: is unwrap the right thing here?
                                last_sent_id = *owned_event.id();
                                event_sender.send(ConsumerMessage::EventLoaded(connection_id, owned_event)).expect("Failed to send EventLoaded message");
                                sent_events += 1;
                            }
                            Err(err) => {
                                error!("Error reading event: {:?}", err);
                                //TODO: send error message to consumer manager instead of just dying silently
                                break;
                            }
                        }
                    }
                    debug!("Finished reader thread for connection_id: {}, sent_events: {}, last_send_event: {:?}", connection_id, sent_events, last_sent_id);
                    if sent_events < limit as usize {
                        let continue_message = ConsumerMessage::ContinueConsuming(connection_id, last_sent_id, limit - sent_events as i64);
                        event_sender.send(continue_message).expect("Failed to send continue_message");
                    }
                    //TODO: else send ConsumerCompleted message
                });

            } else {
                debug!("Sending events from cache for connection: {}", connection_id);
                client.start_consuming(ConsumingState::forward_from_memory(start_id, limit as u64));
                cache.do_with_range(start_id, limit as usize, |(id, event)| {
                    trace!("Sending event from cache. connection_id: {}, event_id: {:?}", connection_id, id);
                    client.send(ServerMessage::Event(event)).unwrap(); //TODO: something better than unwrap
                });
            }
        })
    }

}

pub struct ConsumerMap(HashMap<ConnectionId, Client>);
impl ConsumerMap {
    pub fn new() -> ConsumerMap {
        ConsumerMap(HashMap::with_capacity(32))
    }

    pub fn add(&mut self, connect: ClientConnect) {
        let connection_id = connect.connection_id;
        self.0.insert(connection_id, Client::from_client_connect(connect));
    }

    pub fn remove(&mut self, connection_id: ConnectionId) {
        self.0.remove(&connection_id);
    }

    pub fn get_mut(&mut self, connection_id: ConnectionId) -> Result<&mut Client, String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("No Client exists for connection id: {}", connection_id)
        })
    }

    pub fn get_consumer_position(&self, connection_id: ConnectionId) -> Result<FloEventId, String> {
        self.0.get(&connection_id).ok_or_else(|| {
            format!("Consumer: {} does not exist", connection_id)
        }).map(|consumer| {
            consumer.get_current_position()
        })
    }

    pub fn update_consumer_position(&mut self, connection_id: ConnectionId, new_position: FloEventId) -> Result<(), String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("Consumer: {} does not exist. Cannot update position", connection_id)
        }).map(|consumer| {
            consumer.set_position(new_position);
        })
    }

    pub fn send_event(&mut self, connection_id: ConnectionId, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("Cannot send event to consumer because consumer: {} does not exist", connection_id)
        }).and_then(|mut client| {
            client.send(ServerMessage::Event(event)).map_err(|err| {
                format!("Error sending event to server channel: {:?}", err)
            })
        })
    }

    pub fn send_event_to_all(&mut self, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        for client in self.0.values_mut() {
            trace!("Checking to send event: {:?}, to client: {}, {:?}", event.id(), client.connection_id(), client.is_awaiting_new_event());
            if client.is_awaiting_new_event() {
                client.send(ServerMessage::Event(event.clone())).unwrap();
            }
        }
        Ok(())
    }
}


