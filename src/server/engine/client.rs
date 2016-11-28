use server::engine::api::{ConnectionId, ServerMessage, ClientConnect};

use futures::sync::mpsc::UnboundedSender;

use std::net::SocketAddr;

static SEND_ERROR_DESC: &'static str = "Failed to send message through Client Channel";

#[derive(Debug, PartialEq)]
pub struct ClientSendError(ServerMessage);

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

pub trait Client {
    fn from_client_connect(message: ClientConnect) -> Self;

    fn connection_id(&self) -> ConnectionId;
    fn addr(&self) -> &SocketAddr;
    fn send(&mut self, message: ServerMessage) -> Result<(), ClientSendError>;
}

pub struct ClientImpl {
    connection_id: ConnectionId,
    addr: SocketAddr,
    sender: UnboundedSender<ServerMessage>,
}

impl Client for ClientImpl {

    fn from_client_connect(connect_message: ClientConnect) -> ClientImpl {
        ClientImpl {
            connection_id: connect_message.connection_id,
            addr: connect_message.client_addr,
            sender: connect_message.message_sender,
        }
    }

    fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    fn send(&mut self, message: ServerMessage) -> Result<(), ClientSendError> {
        self.sender.send(message).map_err(|send_err| {
            ClientSendError(send_err.into_inner())
        })
    }
}
