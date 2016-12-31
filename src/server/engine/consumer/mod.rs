mod client;

pub use self::client::{Client, ClientState, ClientSendError};

use server::engine::api::{ConnectionId, ServerMessage, ClientMessage, ClientConnect};
use flo_event::{FloEvent, OwnedFloEvent, FloEventId};

use futures::sync::mpsc::UnboundedSender;
use lru_time_cache::LruCache;

use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::marker::Send;
use std::collections::HashMap;

use server::engine::client_map::ClientMap;
use event_store::EventReader;

const MAX_CACHED_EVENTS: usize = 1024;

pub struct ConsumerManager<R: EventReader + 'static> {
    event_reader: R,
    my_sender: mpsc::Sender<ClientMessage>,
    consumers: ConsumerMap,
    greatest_event_id: FloEventId,
}

impl <R: EventReader + 'static> ConsumerManager<R> {
    pub fn new(reader: R, sender: mpsc::Sender<ClientMessage>, greatest_event_id: FloEventId) -> Self {
        ConsumerManager {
            my_sender: sender,
            event_reader: reader,
            consumers: ConsumerMap::new(),
            greatest_event_id: greatest_event_id,
        }
    }

    pub fn process(&mut self, message: ClientMessage) -> Result<(), String> {
        trace!("Got message: {:?}", message);
        match message {
            ClientMessage::ClientConnect(connect) => {
                self.consumers.add(connect);
                Ok(())
            }
            ClientMessage::StartConsuming(connection_id, limit) => {
                self.start_consuming(connection_id, limit)
            }
            ClientMessage::EventLoaded(connection_id, event) => {
                self.update_greatest_event(event.id);
                self.consumers.send_event(connection_id, Arc::new(event))
            }
            ClientMessage::EventPersisted(connection_id, event) => {
                self.update_greatest_event(event.id);
                warn!("EventPersisted not yet implemented! ConnectionId: {}, Event: {:?}", connection_id, event);
                Ok(())
            }
            m @ _ => {
                panic!("Got unhandled message: {:?}", m);
            }
        }
    }

    fn update_greatest_event(&mut self, id: FloEventId) {
        if id > self.greatest_event_id {
            self.greatest_event_id = id;
        }
    }

    fn start_consuming(&mut self, connection_id: ConnectionId, limit: i64) -> Result<(), String> {
        let ConsumerManager{ref mut event_reader, ref my_sender, ref consumers, ref greatest_event_id} = *self;
        consumers.get_consumer_position(connection_id).map(|start_id| {
            let event_iter = event_reader.load_range(start_id, limit as usize);

            let event_sender = my_sender.clone();
            thread::spawn(move || {
                for event in event_iter {
                    match event {
                        Ok(owned_event) => {
                            trace!("Reader thread sending event: {:?} to consumer manager", owned_event.id());
                            //TODO: is unwrap the right thing here?
                            event_sender.send(ClientMessage::EventLoaded(connection_id, owned_event)).unwrap();
                        }
                        Err(err) => {
                            error!("Error reading event: {:?}", err);
                            //TODO: send error message to consumer manager instead of just dying silently
                            break;
                        }
                    }
                }
            });
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

    pub fn get_consumer_position(&self, connection_id: ConnectionId) -> Result<FloEventId, String> {
        self.0.get(&connection_id).ok_or_else(|| {
            format!("Consumer: {} does not exist", connection_id)
        }).map(|consumer| {
            consumer.get_current_position()
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
}


