use server::engine::api::{ConnectionId, ServerMessage};
use protocol::{ServerProtocol, ServerProtocolImpl};

use futures::stream::Stream;
use futures::{Future, IntoFuture, Async};
use futures::sync::mpsc::UnboundedReceiver;
use tokio_core::net::TcpStream;
use tokio_core::io as nio;
use tokio_core::io::{copy, Copy};

use std::io::{self, Read};

pub struct ServerMessageStream<P: ServerProtocol> {
    connection_id: ConnectionId,
    server_receiver: UnboundedReceiver<ServerMessage>,
    current_message: Option<P>,
}

impl <P: ServerProtocol> ServerMessageStream<P> {
    pub fn new(connection_id: ConnectionId, server_rx: UnboundedReceiver<ServerMessage>) -> ServerMessageStream<P> {
        ServerMessageStream {
            connection_id: connection_id,
            server_receiver: server_rx,
            current_message: None,
        }
    }
}

fn not_ready() -> io::Error {
    static not_ready_msg: &'static str = "Waiting for another message from the server";
    io::Error::new(io::ErrorKind::WouldBlock, not_ready_msg)
}

impl <P: ServerProtocol> Read for ServerMessageStream<P> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut buffer_position = 0usize;
        let mut buffer_remaining = buf.len();
        let mut message = self.current_message.take();

        if message.is_none() {
            // we don't currently have a message to read, so try to get one
            match self.server_receiver.poll() {
                Ok(Async::NotReady) => {
                    return Err(not_ready());
                }
                Ok(Async::Ready(Some(msg))) => {
                    message = Some(P::new(msg))
                }
                Ok(Async::Ready(None)) => {
                    return Ok(0);
                }
                Err(err) => {
                    return Err(io::Error::new(io::ErrorKind::Other, "Failed to read next message from server"));
                }
            }
        }

        if let Some(mut msg) = message {
            //we are current in process of reading this message
            let read_res = msg.read(buf);

            match read_res {
                Ok(nread) if nread > 0 => {
                    //if we've managed to read some bytes, then keep this message around since it may not be done
                    self.current_message = Some(msg);
                    Ok(nread)
                }
                Ok(_) => {
                    //we've finished reading this message, so try later to get another from the backend
                    Err(not_ready())
                }
                e @ Err(_) => e
            }
        } else {
            unreachable!()
        }
    }
}
