use std::io::{self, Read};
use std::fmt::{self, Debug};

use futures::{Future, Async, Poll, Sink, Stream};

use protocol::{self, ProtocolMessage};
use codec::EventCodec;

pub trait MessageStream: Stream<Item=ProtocolMessage, Error=io::Error> + Debug {
}

pub struct MessageRecvStream<R: Read> {
    message_reader: protocol::MessageStream<R>,
    connected: bool
}

impl <R: Read> MessageRecvStream<R> {
    pub fn new(reader: R) -> MessageRecvStream<R> {
        MessageRecvStream {
            message_reader: protocol::MessageStream::new(reader),
            connected: true,
        }
    }
}

impl <R: Read> MessageStream for MessageRecvStream<R> { }

impl <R: Read> Debug for MessageRecvStream<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MessageRecvStream{{ conec")
    }
}

impl <R: Read> Stream for MessageRecvStream<R> {
    type Item = ProtocolMessage;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if !self.connected {
            return Ok(Async::Ready(None));
        }
        match self.message_reader.read_next() {
            Ok(message) => Ok(Async::Ready(Some(message))),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(Async::NotReady),
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.connected = false;
                Ok(Async::Ready(None))
            }
            Err(io_err) => {
                self.connected = false;
                Err(io_err)
            }
        }
    }
}






