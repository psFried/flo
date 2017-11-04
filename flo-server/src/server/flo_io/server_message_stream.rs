use server::engine::api::{ConnectionId};
use protocol::{ProtocolMessage, MessageWriter};

use futures::stream::Stream;
use futures::Async;
use futures::sync::mpsc::UnboundedReceiver;
use futures::{Future, Poll};

#[allow(deprecated)]
use tokio_core::io::WriteHalf;
use tokio_core::net::TcpStream;
use std::io;

#[allow(deprecated)]
pub type ServerWriteStream = WriteHalf<TcpStream>;

pub struct ServerMessageStream {
    connection_id: ConnectionId,
    server_receiver: UnboundedReceiver<ProtocolMessage>,
    current_message: Option<MessageWriter<'static>>,
    tcp_stream: ServerWriteStream,
}

impl ServerMessageStream {
    pub fn new(connection_id: ConnectionId, server_rx: UnboundedReceiver<ProtocolMessage>, tcp_stream: ServerWriteStream) -> ServerMessageStream {
        ServerMessageStream {
            connection_id: connection_id,
            server_receiver: server_rx,
            current_message: None,
            tcp_stream: tcp_stream,
        }
    }

    fn needs_next_message(&self) -> bool {
        self.current_message.as_ref().map(|m| m.is_done()).unwrap_or(true)
    }
}

impl Future for ServerMessageStream {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            if self.needs_next_message() {
                match self.server_receiver.poll() {
                    Ok(Async::Ready(Some(message))) => {
                        self.current_message = Some(MessageWriter::new_owned(message));
                    }
                    Ok(Async::Ready(None)) => {
                        return Ok(Async::Ready(()));
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                    Err(()) => {
                        warn!("Error reading from message receiver for connection_id: {}, Completing Stream", self.connection_id);
                        return Ok(Async::Ready(()));
                    }
                }
            }

            let ServerMessageStream {
                connection_id,
                ref mut current_message,
                ref mut tcp_stream,
                .. } = *self;

            if let Some(ref mut message) = current_message.as_mut() {
                match message.write(tcp_stream) {
                    Ok(()) => {
                        // loop back around for another try
                        trace!("Successfully wrote part of message to connection_id: {}, finished_message: {}", connection_id, message.is_done());
                    }
                    Err(ref io_err) if io_err.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(Async::NotReady);
                    }
                    Err(io_err) => {
                        error!("Error writing message to connection_id: {}, {}", connection_id, io_err);
                        return Err(io_err);
                    }
                }
            }
        }
    }
}

