use futures::{Poll, Async};
use futures::stream::Stream;
use std::io::{self, Read};
use std::time::{Instant};
use std::net::SocketAddr;

use server::engine::api::{self, ClientMessage, ConnectionId};
use protocol::{ClientProtocol, ProtocolMessage, ProduceEventHeader, ConsumerStart, MessageStream};
use nom::IResult;

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
                self.disconnect()
            }
            Err(io_err) => {
                warn!("Error reading data: {:?}", io_err);
                self.disconnect()
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use futures::Async;
    use futures::stream::Stream;
    use std::io::{self, Read, Cursor};

    use event::FloEventId;
    use server::engine::api::{ClientMessage, ConsumerManagerMessage, ProducerManagerMessage};
    use protocol::{ClientProtocol, ClientProtocolImpl, ProtocolMessage, ProduceEvent};
    use nom::{IResult, Needed, ErrorKind};

    fn address() -> SocketAddr {
        "127.0.0.1:3000".parse().unwrap()
    }

    #[test]
    fn multiple_events_are_read_in_sequence() {
        ::env_logger::init();
        let reader = {
            let mut b = Vec::new();
            b.extend_from_slice(b"FLO_PRO\n/foo/bar\n");
            b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 9, 0, 5]);
            b.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 7]);
            b.extend_from_slice(b"evt_one");
            b.extend_from_slice(b"FLO_AUT\n");
            b.extend_from_slice(b"the namespace\n");
            b.extend_from_slice(b"the username\n");
            b.extend_from_slice(b"the password\n");
            b.extend_from_slice(b"FLO_PRO\n/baz\n");
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
        if let Ok(Async::Ready(Some(message))) = result {
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
