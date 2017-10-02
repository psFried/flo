use futures::{Poll, Async};
use futures::stream::Stream;
use std::io::{self, Read};
use std::net::SocketAddr;

use server::engine::api::{ClientMessage, ConnectionId};
use protocol::{MessageStream, ProtocolMessage};

pub struct ClientMessageStream<R: Read> {
    connection_id: ConnectionId,
    client_address: SocketAddr,
    message_reader: MessageStream<R>,
    connected: bool
}

impl <R: Read> ClientMessageStream<R> {
    pub fn new(connection_id: ConnectionId, client_address: SocketAddr, reader: R) -> ClientMessageStream<R> {
        ClientMessageStream {
            connection_id: connection_id,
            client_address: client_address,
            message_reader: MessageStream::new(reader),
            connected: true,
        }
    }

    fn disconnect(&mut self) -> Poll<Option<ClientMessage>, String> {
        if self.connected {
            self.connected = false;
            Ok(Async::Ready(Some(ClientMessage::disconnect(self.connection_id, self.client_address))))
        } else {
            Ok(Async::Ready(None))
        }
    }
}

impl <R: Read> Stream for ClientMessageStream<R> {
    type Item = ClientMessage;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Extra guard in case previous poll returned an error and disconnected
        // otherwise, we might try to read or parse again
        if !self.connected {
            return self.disconnect();
        }

        match self.message_reader.read_next() {
            Ok(message) => {
                Ok(Async::Ready(Some(ClientMessage::from_protocol_message(self.connection_id, message))))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(Async::NotReady),
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                info!("EOF for connection_id: {}, err: {:?}", self.connection_id, e);
                self.disconnect()
            }
            Err(io_err) => {
                warn!("Error reading data: {:?}", io_err);
                self.disconnect()
            }
        }
    }
}

/// New implementation, that just provides a `Stream` of `ProtocolMessage`s.
/// Theres a lot of duplicated code here, at the moment, until `ClientMessageStream` is removed
pub struct ProtocolMessageStream<R: Read> {
    connection_id: ConnectionId,
    message_reader: MessageStream<R>,
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
    type Item = ProtocolMessage;
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


#[cfg(test)]
mod test {
    use super::*;
    use futures::Async;
    use futures::stream::Stream;
    use std::io::Cursor;

    use event::FloEventId;
    use server::engine::api::{ClientMessage, ProducerManagerMessage};
    use protocol::ProtocolMessage;
    use protocol::headers;

    fn address() -> SocketAddr {
        "127.0.0.1:3000".parse().unwrap()
    }

    #[test]
    fn multiple_events_are_read_in_sequence() {
        let reader = {
            let mut b = Vec::new();
            b.push(headers::PRODUCE_EVENT);
            b.extend_from_slice(&[0, 8]);
            b.extend_from_slice(b"/foo/bar");
            b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 9, 0, 5]);
            b.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 7]);
            b.extend_from_slice(b"evt_one");
            b.push(headers::CLIENT_AUTH);
            b.extend_from_slice(&[0, 13]);
            b.extend_from_slice(b"the namespace");
            b.extend_from_slice(&[0, 12]);
            b.extend_from_slice(b"the username");
            b.extend_from_slice(&[0, 12]);
            b.extend_from_slice(b"the password");
            b.push(headers::PRODUCE_EVENT);
            b.extend_from_slice(&[0, 4]);
            b.extend_from_slice(b"/baz");
            b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 9, 0, 5]);
            b.extend_from_slice(&[0, 0, 0, 5, 0, 0, 0, 7]);
            b.extend_from_slice(b"evt_two");
            Cursor::new(b)
        };

        let mut subject = ClientMessageStream::new(123, address(), reader);

        let result = subject.poll();
        if let Ok(Async::Ready(Some(ClientMessage::Producer(ProducerManagerMessage::Receive(received))))) = result {
            assert_eq!(123, received.sender);
            if let ProtocolMessage::ProduceEvent(event) = received.message {
                assert_eq!("/foo/bar", &event.namespace);
                assert_eq!(4, event.op_id);
                assert_eq!(Some(FloEventId::new(5, 9)), event.parent_id);
                assert_eq!("evt_one".as_bytes().to_owned(), event.data);
            } else {
                panic!("wrong message: {:?}", received);
            }
        } else {
            panic!("this result sucks: {:?}", result);
        }

        let result = subject.poll();
        if let Ok(Async::Ready(Some(_message))) = result {
            //TODO: assert that message fields are correct
            println!("Got some message");
        } else {
            panic!("expected to get a message");
        }

        let result = subject.poll();
        if let Ok(Async::Ready(Some(ClientMessage::Producer(ProducerManagerMessage::Receive(received))))) = result {
            assert_eq!(123, received.sender);
            if let ProtocolMessage::ProduceEvent(event) = received.message {
                assert_eq!("/baz", &event.namespace);
                assert_eq!(5, event.op_id);
                assert_eq!(Some(FloEventId::new(5, 9)), event.parent_id);
                assert_eq!("evt_two".as_bytes().to_owned(), event.data);
            } else {
                panic!("received the wrong message: {:?}", received);
            }
        } else {
            panic!("this result sucks: {:?}", result);
        }

        let result = subject.poll();
        let expected = Ok(Async::Ready(Some(ClientMessage::disconnect(123, address()))));
        assert_eq!(expected, result);

        let result = subject.poll();
        let expected = Ok(Async::Ready(None));
        assert_eq!(expected, result);
    }


}
