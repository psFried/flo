use server::engine::api::{ConnectionId, ServerMessage};
use protocol::ServerProtocol;

use futures::stream::Stream;
use futures::Async;
use futures::sync::mpsc::UnboundedReceiver;

use std::io::{self, Read};

pub struct ServerMessageStream<P: ServerProtocol> {
    _connection_id: ConnectionId,
    server_receiver: UnboundedReceiver<ServerMessage>,
    current_message: Option<P>,
}

impl <P: ServerProtocol> ServerMessageStream<P> {
    pub fn new(connection_id: ConnectionId, server_rx: UnboundedReceiver<ServerMessage>) -> ServerMessageStream<P> {
        ServerMessageStream {
            _connection_id: connection_id,
            server_receiver: server_rx,
            current_message: None,
        }
    }
}

fn not_ready() -> io::Error {
    static NOT_READY_MSG: &'static str = "Waiting for another message from the server";
    io::Error::new(io::ErrorKind::WouldBlock, NOT_READY_MSG)
}

impl <P: ServerProtocol> Read for ServerMessageStream<P> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut message = self.current_message.take();

        let next_message_needed = match &message {
            &Some(ref msg) => msg.is_done(),
            _ => true
        };

        if next_message_needed {
            // we don't currently have a message to read, so try to get one
            match self.server_receiver.poll() {
                Ok(Async::NotReady) => {
                    return Err(not_ready());
                }
                Ok(Async::Ready(Some(msg))) => {
                    trace!("ServerMessageStream got new message: {:?}", msg);
                    message = Some(P::new(msg))
                }
                Ok(Async::Ready(None)) => {
                    debug!("End of ServerMessageStream");
                    return Ok(0);
                }
                Err(err) => {
                    error!("Error reading next message from server: {:?}", err);
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
                    trace!("Read {} bytes of current message", nread);
                    self.current_message = Some(msg);
                    Ok(nread)
                }
                Ok(_) => {
                    //we've finished reading this message, so try later to get another from the backend
                    trace!("Finished reading from current message");
                    Err(not_ready())
                }
                e @ Err(_) => e
            }
        } else {
            unreachable!()
        }
    }
}
