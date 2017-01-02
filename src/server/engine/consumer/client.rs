use flo_event::{FloEvent, FloEventId};
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

#[derive(Debug, PartialEq)]
pub enum ConsumeType {
    File,
    Memory,
}

pub struct ConsumingState {
    pub last_event_id: FloEventId,
    pub consume_type: ConsumeType,
    pub remaining: u64,
}

impl ConsumingState {
    pub fn forward_from_file(id: FloEventId, limit: u64) -> ConsumingState {
        ConsumingState {
            last_event_id: id,
            consume_type: ConsumeType::File,
            remaining: limit,
        }
    }

    pub fn forward_from_memory(id: FloEventId, limit: u64) -> ConsumingState {
        ConsumingState {
            last_event_id: id,
            consume_type: ConsumeType::Memory,
            remaining: limit,
        }
    }

    pub fn event_sent(&mut self, id: FloEventId) {
        self.last_event_id = id;
        self.remaining -= 1;
    }
}

pub enum ClientState {
    NotConsuming(FloEventId),
    Consuming(ConsumingState),
}

pub struct Client {
    connection_id: ConnectionId,
    addr: SocketAddr,
    sender: UnboundedSender<ServerMessage>,
    consumer_state: ClientState,
}

impl Client {
    pub fn from_client_connect(connect_message: ClientConnect) -> Client {
        Client {
            connection_id: connect_message.connection_id,
            addr: connect_message.client_addr,
            sender: connect_message.message_sender,
            consumer_state: ClientState::NotConsuming(FloEventId::new(0, 0)),
        }
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn start_consuming(&mut self, state: ConsumingState) {
        self.consumer_state = ClientState::Consuming(state);
    }

    pub fn stop_consuming(&mut self) {
        let event_id = self.get_current_position();
        self.consumer_state = ClientState::NotConsuming(event_id);
    }

    pub fn send(&mut self, message: ServerMessage) -> Result<(), ClientSendError> {
        let conn_id = self.connection_id;
        trace!("Sending message to client: {} : {:?}", conn_id, message);
        if let &ServerMessage::Event(ref event) = &message {
            if let ClientState::Consuming(ref mut state) = self.consumer_state {
                trace!("Client {} about to update marker to: {:?}, remaining_in_batch: {}", conn_id, event.id(), state.remaining);
                state.event_sent(*event.id());
            }
        }
        self.sender.send(message).map_err(|send_err| {
            ClientSendError(send_err.into_inner())
        })
    }

    pub fn is_awaiting_new_event(&self) -> bool {
        match self.consumer_state {
            ClientState::Consuming(ref state) if state.consume_type == ConsumeType::Memory => true,
            _ => false
        }
    }

    pub fn get_current_position(&self) -> FloEventId {
        match self.consumer_state {
            ClientState::NotConsuming(id) => id,
            ClientState::Consuming(ref state) => state.last_event_id
        }
    }
}
