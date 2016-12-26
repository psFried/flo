use flo_event::FloEventId;
use server::engine::api::{ConnectionId, ClientMessage, ClientAuth, ClientConnect, ServerMessage};

use futures::sync::mpsc::UnboundedSender;

use std::net::SocketAddr;


static SEND_ERROR_DESC: &'static str = "Failed to send message through Client Channel";

#[derive(Debug, PartialEq)]
pub struct ClientSendError(pub ServerMessage);

impl ClientSendError {
    fn into_message(self) -> ServerMessage {
        self.0
    }
}

impl ::std::error::Error for ClientSendError {
    fn description(&self) -> &str {
        SEND_ERROR_DESC
    }
}

impl ::std::fmt::Display for ClientSendError {
    fn fmt(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(formatter, "{}", SEND_ERROR_DESC)
    }
}


pub enum ConsumerState {
    NotConsuming(FloEventId),
    ConsumeForward(FloEventId, i64),
}

pub struct Client {
    connection_id: ConnectionId,
    addr: SocketAddr,
    sender: UnboundedSender<ServerMessage>,
    consumer_state: ConsumerState,
}

impl Client {
    pub fn from_client_connect(connect_message: ClientConnect) -> Client {
        Client {
            connection_id: connect_message.connection_id,
            addr: connect_message.client_addr,
            sender: connect_message.message_sender,
            consumer_state: ConsumerState::NotConsuming(FloEventId::new(0, 0)),
        }
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn send(&mut self, message: ServerMessage) -> Result<(), ClientSendError> {
        trace!("Sending message to client: {} : {:?}", self.connection_id, message);
        if let &ServerMessage::Event(_) = &message {
            if let ConsumerState::ConsumeForward(ref id, ref mut count) = self.consumer_state {
                *count += 1;
            }
        }
        self.sender.send(message).map_err(|send_err| {
            ClientSendError(send_err.into_inner())
        })
    }

    pub fn update_marker(&mut self, new_marker: FloEventId) {
        trace!("Client {} updating marker to: {:?}", self.connection_id, new_marker);
        match self.consumer_state {
            ConsumerState::NotConsuming(ref mut marker) => {
                *marker = new_marker;
            }
            ConsumerState::ConsumeForward(ref mut marker, _) => {
                *marker = new_marker;
            }
        };
    }

    pub fn needs_event(&self, event_id: FloEventId) -> bool {
        match self.consumer_state {
            ConsumerState::ConsumeForward(id, count) => {
                event_id > id && count > 0
            }
            _ => false
        }
    }
}
