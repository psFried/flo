use engine::api::{ConnectionId, ClientConnect};
use protocol::{ServerMessage, ProtocolMessage};

use std::collections::HashMap;
use std::net::SocketAddr;

use futures::sync::mpsc::UnboundedSender;

struct Client {
    remote_address: SocketAddr,
    sender: UnboundedSender<ServerMessage>,
}

impl From<ClientConnect> for Client {
    fn from(ClientConnect{client_addr, message_sender, ..}: ClientConnect) -> Self {
        Client {
            remote_address: client_addr,
            sender: message_sender,
        }
    }
}

pub struct ClientMap(HashMap<ConnectionId, Client>);
impl ClientMap {
    pub fn new() -> ClientMap {
        ClientMap(HashMap::with_capacity(32))
    }

    pub fn add(&mut self, client: ClientConnect) {
        let connection_id = client.connection_id;
        self.0.insert(connection_id, client.into());
    }

    pub fn remove(&mut self, client: ConnectionId) {
        self.0.remove(&client);
    }

    pub fn send(&mut self, connection_id: ConnectionId, message: ProtocolMessage) -> Result<(), String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("Client: {} does not exist in producer map", connection_id)
        }).and_then(|client| {
            trace!("Sending to client: {}, message: {:?}", connection_id, message);
            client.sender.send(ServerMessage::Other(message)).map_err(|err| {
                format!("Failed to send to client: {}, addr: {:?}, err: {:?}", connection_id, client.remote_address, err)
            })
        })
    }
}
