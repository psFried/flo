mod client;
mod cache;
mod filecursor;

use engine::api::{ConnectionId, ClientConnect, ConsumerManagerMessage, ReceivedMessage, NamespaceGlob, ConsumerState};
use protocol::{ProtocolMessage, ErrorMessage, ErrorKind, ConsumerStart, ServerMessage};
use event::{FloEvent, OwnedFloEvent, FloEventId, ActorId, VersionVector};
use std::sync::{Arc, mpsc};
use std::thread;
use std::collections::HashMap;

use self::client::{ClientConnection, ConnectionContext, CursorType};
use self::cache::Cache;
use server::MemoryLimit;
use engine::event_store::EventReader;
use futures::sync::mpsc::UnboundedSender;
use channels::Sender;

pub const DEFAULT_BATCH_SIZE: u64 = 10_000;

struct ManagerState<R: EventReader + 'static> {
    event_reader: R,
    my_sender: mpsc::Sender<ConsumerManagerMessage>,
    greatest_event_id: FloEventId,
    cache: Cache,
}

impl <R: EventReader + 'static> ManagerState<R> {

    fn update_greatest_event(&mut self, id: FloEventId) {
        if id > self.greatest_event_id {
            self.greatest_event_id = id;
        }
    }

}

impl <R: EventReader + 'static> ConnectionContext for ManagerState<R> {
    fn start_consuming<S: Sender<ServerMessage> + 'static>(&mut self, mut consumer_state: ConsumerState, client_sender: &S) -> Result<CursorType, String> {
        use std::clone::Clone;

        let last_cache_evicted = self.cache.last_evicted_id();
        let connection_id = consumer_state.connection_id;
        let start_id = consumer_state.version_vector.min();
        debug!("connection_id: {} starting to consume starting at: {:?}, cache last evicted id: {:?}", connection_id, start_id, last_cache_evicted);
        if start_id < last_cache_evicted {
            // Clone the client sender since we'll need to pass it over to the cursor's thread
            let my_sender = self.my_sender.clone();
            self::filecursor::start(consumer_state, (*client_sender).clone(), &mut self.event_reader, my_sender).map(|cursor| {
                CursorType::File(Box::new(cursor))
            }).map_err(|io_err| {
                error!("Failed to create file cursor for consumer: {:?}, err: {:?}", connection_id, io_err);
                format!("Error creating cursor: {}", io_err)
            })
        } else {
            debug!("Sending events from cache for connection_id: {}", connection_id);

            for (ref id, ref event) in self.cache.iter(start_id) {
                if consumer_state.should_send_event(&*event) {
                    trace!("Sending event from cache. connection_id: {}, event_id: {:?}", connection_id, id);
                    client_sender.send(ServerMessage::Event((*event).clone())).map_err(|_| {
                        error!("Failed to send event: {} to client sender for connection_id: {}", id, connection_id);
                        format!("Failed to send event: {} to client sender for connection_id: {}", id, connection_id)
                    })?; //early return with error if send fails
                    consumer_state.event_sent(**id); // update consumer state
                } else {
                    trace!("Not sending event: {:?} to connection_id: {} due to mismatched namespace", id, connection_id);
                }
                // if the batch is not yet exhausted, then keep going
                if consumer_state.is_batch_exhausted() {
                    break;
                }
            }

            if consumer_state.is_batch_exhausted() {
                trace!("connection_id: {} exhausted batch", connection_id);
                client_sender.send(ServerMessage::Other(ProtocolMessage::EndOfBatch)).map_err(|send_err| {
                    warn!("Failed to send EndOfBatch for connection_id: {}", connection_id);
                    format!("Failed to send EndOfBatch for connection_id: {}", connection_id)
                })?; //return early if we can't send messages to the client
            } else {
                trace!("connection_id: {} reached end of stream and is awaiting new events", connection_id);
                client_sender.send(ServerMessage::Other(ProtocolMessage::AwaitingEvents)).map_err(|send_err| {
                    warn!("Failed to send AwaitingEvents for connection_id: {}", connection_id);
                    format!("Failed to send AwaitingEvents for connection_id: {}", connection_id)
                })?;
            }

            debug!("Finished sending events from cache for consumer: {:?}", consumer_state);
            Ok(CursorType::InMemory(consumer_state))
        }
    }
} 

pub struct ConsumerManager<R: EventReader + 'static> {
    consumers: ConsumerMap<UnboundedSender<ServerMessage>>,
    state: ManagerState<R>,
    default_batch_size: u64,
}

impl <R: EventReader + 'static> ConsumerManager<R> {
    pub fn new(reader: R, sender: mpsc::Sender<ConsumerManagerMessage>, greatest_event_id: FloEventId, max_cached_events: usize, max_cache_memory: MemoryLimit) -> Self {
        ConsumerManager {
            consumers: ConsumerMap::new(),
            state: ManagerState {
                my_sender: sender,
                event_reader: reader,
                greatest_event_id: greatest_event_id,
                cache: Cache::new(max_cached_events, max_cache_memory, greatest_event_id),
            },
            default_batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn process(&mut self, message: ConsumerManagerMessage) -> Result<(), String> {
        match message {
            ConsumerManagerMessage::Connect(connect) => {
                let ClientConnect {connection_id, client_addr, message_sender} = connect;

                let consumer = ClientConnection::new(connection_id, client_addr, message_sender, self.default_batch_size);
                self.consumers.add(consumer);
                Ok(())
            }
            ConsumerManagerMessage::Disconnect(connection_id, client_address) => {
                debug!("Removing client: {} at address: {}", connection_id, client_address);
                self.consumers.remove(connection_id);
                Ok(())
            }
            ConsumerManagerMessage::EventPersisted(_connection_id, event) => {
                self.state.update_greatest_event(event.id);
                let event_rc = self.state.cache.insert(event);
                self.consumers.send_event_to_all(event_rc)
            }
            ConsumerManagerMessage::FileCursorExhausted(consumer_state) => {
                let ConsumerManager{ref mut consumers, ref mut state, ..} = *self;
                let connection_id = consumer_state.connection_id;
                consumers.get_mut(connection_id).and_then(|client| {
                    client.continue_cursor(consumer_state, state).map_err(|()| {
                        format!("Unable to Continue cursor for conneciton_id: {}", connection_id)
                    })
                })
            }
            ConsumerManagerMessage::Receive(received_message) => {
                self.process_received_message(received_message)
            }
        }
    }

    fn process_received_message(&mut self, ReceivedMessage{sender, message, ..}: ReceivedMessage) -> Result<(), String> {
        let ConsumerManager{ref mut consumers, ref mut state, ..} = *self;
        consumers.get_mut(sender).and_then(|client| {
            client.message_received(message, state).map_err(|()| {
                format!("Error processing message for connection_id: {}", client.connection_id)
            })
        })
    }



}

pub struct ConsumerMap<S: Sender<ServerMessage> + 'static>(HashMap<ConnectionId, ClientConnection<S>>);


impl <S: Sender<ServerMessage> + 'static> ConsumerMap<S> {
    pub fn new() -> ConsumerMap<S> {
        ConsumerMap(HashMap::with_capacity(32))
    }

    pub fn add(&mut self, conn: ClientConnection<S>) {
        let connection_id = conn.connection_id;
        self.0.insert(connection_id, conn);
    }

    pub fn remove(&mut self, connection_id: ConnectionId) {
        self.0.remove(&connection_id);
    }

    pub fn get_mut(&mut self, connection_id: ConnectionId) -> Result<&mut ClientConnection<S>, String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("No Client exists for connection id: {}", connection_id)
        })
    }

    pub fn send_event_to_all(&mut self, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        for client in self.0.values_mut() {
            let result = client.maybe_send_event(&event);
            if let Err(()) = result {
                warn!("Failed to send event: {} to connection_id: {}", event.id(), client.connection_id);
            }
        }
        Ok(())
    }
}
