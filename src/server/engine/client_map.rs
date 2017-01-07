use server::engine::api::{ConnectionId, ClientConnect};
use flo_event::{OwnedFloEvent};
use protocol::ServerMessage;

use std::sync::Arc;
use std::collections::HashMap;

pub struct ClientMap(HashMap<ConnectionId, ClientConnect>);
impl ClientMap {
    pub fn new() -> ClientMap {
        ClientMap(HashMap::with_capacity(32))
    }

    pub fn add(&mut self, client: ClientConnect) {
        let connection_id = client.connection_id;
        self.0.insert(connection_id, client);
    }

    pub fn remove(&mut self, client: ConnectionId) {
        self.0.remove(&client);
    }

    pub fn send(&mut self, client: ConnectionId, message: ServerMessage<Arc<OwnedFloEvent>>) -> Result<(), String> {
        self.0.get_mut(&client).ok_or_else(|| {
            format!("Client: {} does not exist in producer map", client)
        }).and_then(|client_connect| {
            client_connect.message_sender.send(message).map_err(|err| {
                format!("Failed to send to client: {}, addr: {:?}, err: {:?}", client, client_connect.client_addr, err)
            })
        })
    }
}
