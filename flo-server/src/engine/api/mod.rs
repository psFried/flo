mod consumer;
mod namespace;

use protocol::ProtocolMessage;
use event::OwnedFloEvent;

use futures::sync::mpsc::UnboundedSender;

use std::time::Instant;
use std::sync::atomic;
use std::fmt::{self, Debug};
use std::net::SocketAddr;

pub use self::consumer::{ConsumerState, ConsumerFilter};
pub use self::namespace::NamespaceGlob;
pub type ConnectionId = usize;

static CURRENT_CONNECTION_ID: atomic::AtomicUsize = atomic::ATOMIC_USIZE_INIT;

pub fn next_connection_id() -> ConnectionId {
    CURRENT_CONNECTION_ID.fetch_add(1, atomic::Ordering::SeqCst)
}

#[derive(Debug, PartialEq, Clone)]
pub enum ClientMessage {
    Consumer(ConsumerManagerMessage),
    Producer(ProducerManagerMessage),
    Both(ConsumerManagerMessage, ProducerManagerMessage),
}

impl ClientMessage {

    pub fn connect(connection_id: ConnectionId, address: SocketAddr, channel: UnboundedSender<ProtocolMessage>) -> ClientMessage {
        let connect = ClientConnect {
            connection_id: connection_id,
            client_addr: address,
            message_sender: channel
        };
        ClientMessage::Both(ConsumerManagerMessage::Connect(connect.clone()), ProducerManagerMessage::Connect(connect))
    }

    pub fn disconnect(connection_id: ConnectionId, address: SocketAddr) -> ClientMessage {
        ClientMessage::Both(ConsumerManagerMessage::Disconnect(connection_id, address), ProducerManagerMessage::Disconnect(connection_id, address))
    }

    pub fn from_protocol_message(connection_id: ConnectionId, protocol_message: ProtocolMessage) -> ClientMessage {
        match protocol_message {
            m @ ProtocolMessage::PeerAnnounce(_) => producer_message(connection_id, m),
            m @ ProtocolMessage::ProduceEvent(_) => producer_message(connection_id, m),
            m @ ProtocolMessage::Error(_) => producer_message(connection_id, m),
            m @ ProtocolMessage::PeerUpdate {..} => producer_message(connection_id, m),
            m @ ProtocolMessage::AwaitingEvents => producer_message(connection_id, m),
            m @ ProtocolMessage::ReceiveEvent(_) => producer_message(connection_id, m),
            m @ ProtocolMessage::EndOfBatch => producer_message(connection_id, m),
            m @ ProtocolMessage::CursorCreated(_) => producer_message(connection_id, m),

            m @ ProtocolMessage::AckEvent(_) => consumer_message(connection_id, m),
            m @ ProtocolMessage::StartConsuming(_) => consumer_message(connection_id, m),
            m @ ProtocolMessage::StopConsuming => consumer_message(connection_id, m),
            m @ ProtocolMessage::UpdateMarker(_) => consumer_message(connection_id, m),
            m @ ProtocolMessage::SetBatchSize(_) => consumer_message(connection_id, m),
            m @ ProtocolMessage::NextBatch => consumer_message(connection_id, m),

            m @ ProtocolMessage::ClientAuth {..} => both(connection_id, m),

            _ => unimplemented!()
        }
    }
}

fn producer_message(connection_id: ConnectionId, protocol_message: ProtocolMessage) -> ClientMessage {
    ClientMessage::Producer(ProducerManagerMessage::Receive(ReceivedMessage::received_now(connection_id, protocol_message)))
}

fn consumer_message(connection_id: ConnectionId, protocol_message: ProtocolMessage) -> ClientMessage {
    ClientMessage::Consumer(ConsumerManagerMessage::Receive(ReceivedMessage::received_now(connection_id, protocol_message)))
}

fn both(connection_id: ConnectionId, protocol_message: ProtocolMessage) -> ClientMessage {
    let recv_message = ReceivedMessage::received_now(connection_id, protocol_message);
    ClientMessage::Both(ConsumerManagerMessage::Receive(recv_message.clone()), ProducerManagerMessage::Receive(recv_message))
}


#[derive(Clone)]
pub struct ClientConnect {
    pub connection_id: ConnectionId,
    pub client_addr: SocketAddr,
    pub message_sender: UnboundedSender<ProtocolMessage>,
}

impl Debug for ClientConnect {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(formatter, "NewClient {{ connection_id: {}, client_addr: {:?} }}", self.connection_id, self.client_addr)
    }
}

impl PartialEq for ClientConnect {
    fn eq(&self, other: &ClientConnect) -> bool {
        self.connection_id == other.connection_id &&
                self.client_addr == other.client_addr &&
                &(self.message_sender) as * const UnboundedSender<ProtocolMessage> == &(other.message_sender) as * const UnboundedSender<ProtocolMessage>
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReceivedMessage {
    pub sender: ConnectionId,
    pub recv_time: Instant,
    pub message: ProtocolMessage
}

impl ReceivedMessage {
    pub fn received_now(connection_id: ConnectionId, message: ProtocolMessage) -> ReceivedMessage {
        ReceivedMessage {
            sender: connection_id,
            recv_time: Instant::now(),
            message: message
        }
    }
}


#[derive(Debug, PartialEq, Clone)]
pub enum ConsumerManagerMessage {
    Connect(ClientConnect),
    Disconnect(ConnectionId, SocketAddr),
    EventPersisted(ConnectionId, OwnedFloEvent),
    FileCursorExhausted(ConsumerState),
    Receive(ReceivedMessage),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ProducerManagerMessage {
    Connect(ClientConnect),
    OutgoingConnectFailure(SocketAddr),
    Disconnect(ConnectionId, SocketAddr),
    Tick,
    Receive(ReceivedMessage),
}

