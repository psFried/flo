mod client;

pub use self::client::{Client, ConsumerState, ClientSendError};

use server::engine::api::{ConnectionId, ServerMessage, ClientConnect};
use flo_event::{FloEvent, OwnedFloEvent, FloEventId};

use futures::sync::mpsc::UnboundedSender;
use lru_time_cache::LruCache;

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::thread::{self, JoinHandle};

const MAX_CACHED_EVENTS: usize = 1024;


pub trait ClientManager {
    fn add_connection(&mut self, client_connect: ClientConnect);
    fn send_event(&mut self, event_producer: ConnectionId, event: OwnedFloEvent);
    fn send_message(&mut self, recipient: ConnectionId, message: ServerMessage) -> Result<(), ClientSendError>;
    fn update_marker(&mut self, connection: ConnectionId, marker: FloEventId);
}

pub struct ClientManagerImpl {
    client_map: HashMap<ConnectionId, Client>,
    event_cache: LruCache<FloEventId, Arc<OwnedFloEvent>>,
}

impl ClientManagerImpl {
    pub fn new() -> ClientManagerImpl {
        ClientManagerImpl {
            client_map: HashMap::with_capacity(128),
            event_cache: LruCache::with_capacity(MAX_CACHED_EVENTS),
        }
    }
}

impl ClientManager for ClientManagerImpl {

    fn add_connection(&mut self, client_connect: ClientConnect) {
        let connection_id = client_connect.connection_id;
        let client_count = self.client_map.len() + 1;
        debug!("Adding Client with connection_id: {}, peer_addr: {} total_connections_open: {}",
               connection_id,
               &client_connect.client_addr,
               client_count);
        let client = Client::from_client_connect(client_connect);
        self.client_map.insert(connection_id, client);
    }

    fn send_event(&mut self, event_producer: ConnectionId, event: OwnedFloEvent) {
        let event_id = event.id;
        let event_arc = Arc::new(event);
        let mut clients_to_remove = Vec::new();
        for mut client in self.client_map.values_mut() {
            let client_id = client.connection_id();
            if client_id != event_producer && client.needs_event(event_id) {
                debug!("Sending event: {:?} to client: {}", event_id, client_id);
                if let Err(err) = client.send(ServerMessage::Event(event_arc.clone())) {
                    warn!("Failed to send event: {:?} through client channel. Client likely just disconnected. ConnectionId: {}",
                          event_id,
                          client_id);
                    clients_to_remove.push(client_id);
                } else {
                    debug!("sent event: {:?} to client channel: {}", event_id, client_id);
                }
            }
        }

        // if we were unable to send messages to any clients, then remove them since the connection is probably now closed anyway
        for id in clients_to_remove {
            self.client_map.remove(&id);
        }
    }

    fn send_message(&mut self, connection_id: ConnectionId, message: ServerMessage) -> Result<(), ClientSendError> {
        match self.client_map.get_mut(&connection_id) {
            Some(client) => client.send(message),
            None => Err(ClientSendError(message))
        }
    }

    fn update_marker(&mut self, connection_id: ConnectionId, marker: FloEventId) {
        self.client_map.get_mut(&connection_id).map(|client| {
            client.update_marker(marker)
        });
    }
}
