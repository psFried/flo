use futures::{Poll, Async};
use futures::stream::Stream;
use std::io::{self, Read};
use std::time::{Instant};
use std::net::SocketAddr;

use server::engine::api::{self, ClientMessage, ProducerMessage, ConsumerMessage, ConnectionId, PeerVersionMap};
use protocol::{ClientProtocol, ProtocolMessage, ProduceEventHeader, ConsumerStart, MessageReader};
use nom::IResult;

pub struct ClientMessageStream<R: Read> {
    connection_id: ConnectionId,
    client_address: SocketAddr,
    message_reader: MessageReader<R>,
    connected: bool
}

impl <R: Read> ClientMessageStream<R> {
    pub fn new(connection_id: ConnectionId, client_address: SocketAddr, reader: R) -> ClientMessageStream<R> {
        ClientMessageStream {
            connection_id: connection_id,
            client_address: client_address,
            message_reader: MessageReader::new(reader),
            connected: true,
        }
    }

    fn disconnect_message(&self) -> ClientMessage {
        ClientMessage::Both(ConsumerMessage::Disconnect(self.connection_id, self.client_address.clone()),
                            ProducerMessage::Disconnect(self.connection_id, self.client_address.clone()))
    }


    fn disconnect(&mut self) -> Poll<Option<ClientMessage>, String> {
        if self.connected {
            self.connected = false;
            Ok(Async::Ready(Some(self.disconnect_message())))
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
                let api_message = to_engine_api_message(message, self.connection_id);
                Ok(Async::Ready(Some(api_message)))
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

fn to_engine_api_message(protocol_message: ProtocolMessage, connection_id: ConnectionId) -> ClientMessage {
    match protocol_message {
        ProtocolMessage::StartConsuming(ConsumerStart {namespace, max_events}) => {
            ClientMessage::Consumer(ConsumerMessage::StartConsuming(connection_id, namespace, max_events))
        },
        ProtocolMessage::ClientAuth {namespace, username, password} => {
            let auth = api::ClientAuth {
                connection_id: connection_id,
                namespace: namespace,
                username: username,
                password: password,
            };
            ClientMessage::Both(ConsumerMessage::ClientAuth(auth.clone()), ProducerMessage::ClientAuth(auth))
        }
        ProtocolMessage::UpdateMarker(event_id) => {
            ClientMessage::Consumer(ConsumerMessage::UpdateMarker(connection_id, event_id))
        },
        ProtocolMessage::PeerAnnounce(actor_id, version_vec) => {
            let peer_versions = PeerVersionMap {
                connection_id: connection_id,
                from_actor: actor_id,
                actor_versions: version_vec,
            };
            ClientMessage::Producer(ProducerMessage::PeerAnnounce(peer_versions))
        }
        m @ _ => {
            panic!("Unexpected protocol message: {:?}", m)
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
    use server::engine::api::{ClientMessage, ConsumerMessage, ProducerMessage, ClientAuth};
    use protocol::{ClientProtocol, ClientProtocolImpl, ProtocolMessage, ProduceEventHeader};
    use nom::{IResult, Needed, ErrorKind};

    fn address() -> SocketAddr {
        "127.0.0.1:3000".parse().unwrap()
    }

    #[test]
    fn multiple_events_are_read_in_sequence() {
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
        if let Ok(Async::Ready(Some(ClientMessage::Producer(ProducerMessage::Produce(event))))) = result {
            assert_eq!("/foo/bar", &event.namespace);
            assert_eq!(4, event.op_id);
            assert_eq!(Some(FloEventId::new(5, 9)), event.parent_id);
            assert_eq!(123, event.connection_id);
            assert_eq!("evt_one".as_bytes().to_owned(), event.event_data);
        } else {
            panic!("this result sucks: {:?}", result);
        }

        let result = subject.poll();
        let expected_auth = ClientAuth {
            connection_id: 123,
            namespace: "the namespace".to_owned(),
            username: "the username".to_owned(),
            password: "the password".to_owned()
        };
        let expected = Ok(Async::Ready(Some(ClientMessage::Both(
                ConsumerMessage::ClientAuth(expected_auth.clone()),
                ProducerMessage::ClientAuth(expected_auth.clone())
        ))));
        assert_eq!(expected, result);

        let result = subject.poll();
        if let Ok(Async::Ready(Some(ClientMessage::Producer(ProducerMessage::Produce(event))))) = result {
            assert_eq!("/baz", &event.namespace);
            assert_eq!(5, event.op_id);
            assert_eq!(Some(FloEventId::new(5, 9)), event.parent_id);
            assert_eq!(123, event.connection_id);
            assert_eq!("evt_two".as_bytes().to_owned(), event.event_data);
        } else {
            panic!("this result sucks: {:?}", result);
        }

        let result = subject.poll();
        let expected = Ok(Async::Ready(Some(subject.disconnect_message())));
        assert_eq!(expected, result);

        let result = subject.poll();
        let expected = Ok(Async::Ready(None));
        assert_eq!(expected, result);
    }


}
