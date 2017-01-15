use flo_event::{FloEvent, FloEventId, OwnedFloEvent};
use server::engine::api::{ConnectionId, ClientConnect};
use protocol::ServerMessage;

use futures::sync::mpsc::UnboundedSender;

use std::sync::Arc;
use std::net::SocketAddr;


static SEND_ERROR_DESC: &'static str = "Failed to send message through Client Channel";

#[derive(Debug, PartialEq)]
pub struct ClientSendError(pub ServerMessage<Arc<OwnedFloEvent>>);

impl ClientSendError {
    fn into_message(self) -> ServerMessage<Arc<OwnedFloEvent>> {
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
    pub namespace: String,
}

impl ConsumingState {
    pub fn forward_from_file(id: FloEventId, namespace: String, limit: u64) -> ConsumingState {
        ConsumingState {
            last_event_id: id,
            consume_type: ConsumeType::File,
            remaining: limit,
            namespace: namespace
        }
    }

    pub fn forward_from_memory(id: FloEventId, namespace: String, limit: u64) -> ConsumingState {
        ConsumingState {
            namespace: namespace,
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

impl ClientState {
    fn update_event_sent(&mut self, id: FloEventId, connection_id: ConnectionId) {
        let mut new_state: Option<ClientState> = None;

        match self {
            &mut ClientState::Consuming(ref state) if state.remaining == 1 => {
                // this is the last event, so transition to NotConsuming
                new_state = Some(ClientState::NotConsuming(state.last_event_id));
            }
            &mut ClientState::Consuming(ref mut state) => {
                trace!("updating connection_id: {}, last_event_id: {:?}, remaining: {}", connection_id, state.last_event_id, state.remaining);
                state.last_event_id = id;
                state.remaining -= 1;
            }
            &mut ClientState::NotConsuming(_) => {
                unreachable!() //TODO: maybe redesign this to provide a better guarantee that no event will be sent while in NotConsuming state
            }
        }

        if let Some(state) = new_state {
            *self = state;
        }
    }
}

pub struct Client {
    connection_id: ConnectionId,
    addr: SocketAddr,
    sender: UnboundedSender<ServerMessage<Arc<OwnedFloEvent>>>,
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

    pub fn event_namespace_matches(&self, event_namespace: &str) -> bool {
        if let ClientState::Consuming(ref state) = self.consumer_state {
            event_namespace == &state.namespace
        } else {
            false
        }
    }

    pub fn start_consuming(&mut self, state: ConsumingState) {
        self.consumer_state = ClientState::Consuming(state);
    }

    pub fn stop_consuming(&mut self) {
        let event_id = self.get_current_position();
        self.consumer_state = ClientState::NotConsuming(event_id);
    }

    pub fn send(&mut self, message: ServerMessage<Arc<OwnedFloEvent>>) -> Result<(), ClientSendError> {
        let conn_id = self.connection_id;
        trace!("Sending message to client: {} : {:?}", conn_id, message);
        if let &ServerMessage::Event(ref event) = &message {
            self.consumer_state.update_event_sent(*event.id(), self.connection_id);
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

    pub fn set_position(&mut self, new_position: FloEventId) {
        match self.consumer_state {
            ClientState::NotConsuming(ref mut id) => *id = new_position,
            ClientState::Consuming(ref mut state) => {
                debug!("Client: {} moving position from {:?} to {:?} while in consuming state", self.connection_id, state.last_event_id, new_position);
                state.last_event_id = new_position;
            }
        }
    }
}
