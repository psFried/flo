use std::io::{self, Read};

use futures::{Poll, Async};
use futures::stream::Stream;

use event::OwnedFloEvent;
use engine::{ConnectionId, ReceivedProtocolMessage};
use protocol::MessageStream;


/// New implementation, that just provides a `Stream` of `ProtocolMessage`s.
/// Theres a lot of duplicated code here, at the moment, until `ClientMessageStream` is removed
pub struct ProtocolMessageStream<R: Read> {
    connection_id: ConnectionId,
    message_reader: MessageStream<R, OwnedFloEvent>,
    connected: bool
}

impl <R: Read> ProtocolMessageStream<R> {
    pub fn new(connection_id: ConnectionId, reader: R) -> ProtocolMessageStream<R> {
        ProtocolMessageStream {
            connection_id: connection_id,
            message_reader: MessageStream::new(reader),
            connected: true,
        }
    }
}

impl <R: Read> Stream for ProtocolMessageStream<R> {
    type Item = ReceivedProtocolMessage;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Extra guard in case previous poll returned an error and disconnected
        // otherwise, we might try to read or parse again
        if !self.connected {
            return Ok(Async::Ready(None));
        }

        match self.message_reader.read_next() {
            Ok(message) => {
                Ok(Async::Ready(Some(message)))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(Async::NotReady),
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                info!("EOF for connection_id: {}", self.connection_id);
                Ok(Async::Ready(None))
            }
            Err(io_err) => {
                warn!("Error reading data for connection_id: {}: {:?}", self.connection_id, io_err);
                self.connected = false;
                Err(io_err)
            }
        }
    }
}

