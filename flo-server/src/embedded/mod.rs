//! For running an event stream server in-process and using an in-memory transport for communication with it.
//! This is especially useful in development and testing, as it allows an application to run without a dependency
//! on an external server.

use std::fmt::Debug;
use std::io;

use tokio_core::reactor::{Handle, Remote};
use futures::{Stream, Sink, StartSend, AsyncSink, Async, Poll};

use protocol::ProtocolMessage;
use flo_client_lib::async::{AsyncConnection, MessageReceiver, MessageSender, ClientProtocolMessage};
use flo_client_lib::codec::EventCodec;
use event::FloEvent;
use engine::{EngineRef, create_client_channels, start_controller, ConnectionHandler, SendProtocolMessage};

pub use engine::ControllerOptions;
pub use engine::event_stream::EventStreamOptions;


#[derive(Clone, Debug)]
pub struct EmbeddedFloServer {
    engine_ref: EngineRef,
}

impl EmbeddedFloServer {

    pub fn connect_client<D: Debug>(&self, name: String, codec: Box<EventCodec<EventData=D>>, handle: Handle) -> AsyncConnection<D> {
        let engine_ref = self.engine_ref.clone();
        let connection_id = engine_ref.next_connection_id();
        let (client_sender, client_receiver) = create_client_channels();

        let connection_handler = ConnectionHandler::new(connection_id,
                                                            client_sender.clone(),
                                                            engine_ref,
                                                             handle);
        let embedded_connection = EmbeddedConnectionHandler {
            inner: connection_handler
        };

        let receiver = client_receiver.map(|message| {
            message_to_owned(message)
        }).map_err(|recv_err| {
            io::Error::new(io::ErrorKind::UnexpectedEof, format!("Error reading from channel: {:?}", recv_err))
        });
        let recv = Box::new(receiver) as MessageReceiver;
        let send = Box::new(embedded_connection) as MessageSender;

        AsyncConnection::new(name, send, recv, codec)
    }
}

// ugh, this is an annoying copy, because of the need to change the server's event type into that of the client.
// We could just make `AsyncConnection` generic over received event type in order to avoid this, but should
// probably figure out a way to avoid exposing the generic types via the public api. Seems like a 'later' problem
fn message_to_owned(server_msg: SendProtocolMessage) -> ClientProtocolMessage {
    match server_msg {
        ProtocolMessage::ReceiveEvent(event) => ProtocolMessage::ReceiveEvent(event.to_owned_event()),
        ProtocolMessage::StopConsuming(op) => ProtocolMessage::StopConsuming(op),
        ProtocolMessage::AwaitingEvents => ProtocolMessage::AwaitingEvents,
        ProtocolMessage::Error(op) => ProtocolMessage::Error(op),
        ProtocolMessage::StreamStatus(op) => ProtocolMessage::StreamStatus(op),
        ProtocolMessage::AckEvent(op) => ProtocolMessage::AckEvent(op),
        ProtocolMessage::ProduceEvent(op) => ProtocolMessage::ProduceEvent(op),
        ProtocolMessage::NextBatch => ProtocolMessage::NextBatch,
        ProtocolMessage::EndOfBatch => ProtocolMessage::EndOfBatch,
        ProtocolMessage::NewStartConsuming(op) => ProtocolMessage::NewStartConsuming(op),
        ProtocolMessage::CursorCreated(op) => ProtocolMessage::CursorCreated(op),
        ProtocolMessage::Announce(op) => ProtocolMessage::Announce(op),
        ProtocolMessage::SetEventStream(op) => ProtocolMessage::SetEventStream(op),
        ProtocolMessage::PeerAnnounce(op) => ProtocolMessage::PeerAnnounce(op),
        ProtocolMessage::SystemAppendCall(op) => ProtocolMessage::SystemAppendCall(op),
        ProtocolMessage::SystemAppendResponse(op) => ProtocolMessage::SystemAppendResponse(op),
        ProtocolMessage::RequestVote(op) => ProtocolMessage::RequestVote(op),
        ProtocolMessage::VoteResponse(op) => ProtocolMessage::VoteResponse(op),
        ProtocolMessage::CommitIndexUpdated(id) => ProtocolMessage::CommitIndexUpdated(id),
    }
}

pub fn run_embedded_server(options: ControllerOptions, remote: Remote) -> io::Result<EmbeddedFloServer> {
    start_controller(options, remote).map(|engine_ref| {
        EmbeddedFloServer {
            engine_ref: engine_ref,
        }
    })
}

struct EmbeddedConnectionHandler {
    inner: ConnectionHandler
}

impl Sink for EmbeddedConnectionHandler {
    type SinkItem = ClientProtocolMessage;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.inner.start_send(item.into()).map(|async_sink| {
            async_sink.map(|input| input.unwrap_protocol_message())
        })
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.inner.poll_complete()
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.inner.close()
    }
}
