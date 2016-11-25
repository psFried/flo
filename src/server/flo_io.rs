use futures::{Future, Poll, Async};
use futures::stream::Stream;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpStream, TcpStreamNew, TcpListener, Incoming};
use tokio_core::io as nio;
use tokio_core::io::Io;
use std::io::{Read, Write, BufRead};
use std::sync::mpsc::Sender;

use server::engine::api::{self, ClientMessage, ClientConnect, ConnectionId};
use protocol::{ClientProtocol, ProtocolMessage};
use nom::IResult;

pub struct ServerMessageStream {
    connection_id: ConnectionId,
    tcp_writer: nio::WriteHalf<TcpStream>,
}

#[derive(Debug, PartialEq, Eq)]
enum MessageStreamState {
    Reading,
    Parsing,
}

pub struct ClientMessageStream<R: Read, P: ClientProtocol> {
    connection_id: ConnectionId,
    tcp_reader: R,
    protocol: P,
    buffer: Vec<u8>,
    buffer_pos: usize,
    state: MessageStreamState,
}

impl <R: Read, P: ClientProtocol> ClientMessageStream<R, P> {
    pub fn with_next_connection_id(reader: R, protocol: P) -> ClientMessageStream<R, P> {
        let conn_id = api::next_connection_id();
        ClientMessageStream {
            connection_id: conn_id,
            tcp_reader: reader,
            protocol: protocol,
            buffer: vec![0, 8 * 1024],
            buffer_pos: 0,
            state: MessageStreamState::Reading,
        }
    }
}

impl <R: Read, P: ClientProtocol> Stream for ClientMessageStream<R, P> {
    type Item = ClientMessage;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let ClientMessageStream {
            ref connection_id,
            ref mut tcp_reader,
            ref mut buffer,
            ref mut buffer_pos,
            ref protocol,
            ref mut state,
        } = *self;


        if *state == MessageStreamState::Reading {
            let nread = match tcp_reader.read(buffer) {
                Ok(amount) => amount,
                Err(ref err) if err.kind() == ::std::io::ErrorKind::WouldBlock => {
                    return Ok(Async::NotReady);
                },
                Err(err) => {
                    return Err(format!("Error reading from stream: {:?}", err));
                }
            };

            // Reset the buffer position after successful read
            *buffer_pos = 0;
            ::std::mem::replace(state, MessageStreamState::Parsing);
        }

        //Now try to parse
        match protocol.parse_any(&buffer[..]) {
            IResult::Done(remaining, message) => {
                match message {
                    ProtocolMessage::ApiMessage(client_msg) => Ok(Async::Ready(Some(client_msg))),
                    ProtocolMessage::ProduceEvent{data_length} => {
                        panic!("still need to implement produceEvent");
                    }
                }
            }
            IResult::Incomplete(needed) => {
                Err(format!("Got incomplete result, needed: {:?}", needed))
            }
            IResult::Error(err) => {
                Err(format!("Error parsing: {:?}", err))
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use futures::{Future, Poll, Async};
    use futures::stream::Stream;
    use tokio_core::reactor::Core;
    use tokio_core::net::{TcpStream, TcpStreamNew, TcpListener, Incoming};
    use tokio_core::io as nio;
    use tokio_core::io::Io;
    use std::io::{Read, Write, BufRead, Cursor};
    use std::sync::mpsc::Sender;

    use server::engine::api::{self, ClientMessage, ClientConnect, ClientAuth, ConnectionId};
    use protocol::{ClientProtocol, ClientProtocolImpl, ProtocolMessage};
    use nom::IResult;

    #[test]
    fn poll_returns_message_header_when_read_and_parse_both_succeed() {
        let bytes = b"the input bytes plus some other bytes";
        let reader = Cursor::new(&bytes[..]);

        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                IResult::Done(&buffer[..14], ProtocolMessage::ApiMessage(ClientMessage::ClientAuth(ClientAuth{
                    namespace: "theNamespace".to_owned(),
                    username: "theUsername".to_owned(),
                    password: "thePassword".to_owned(),
                })))
            }
        }
        let mut subject = ClientMessageStream::with_next_connection_id(reader, Proto);

        let result = subject.poll().expect("Expected Ok, got Err");

        match result {
            Async::Ready(message) => {
                match message {
                    Some(ClientMessage::ClientAuth(connect)) => {
                        assert_eq!("theUsername", &connect.username);
                        assert_eq!("thePassword", &connect.password);
                        assert_eq!("theNamespace", &connect.namespace);
                    }
                    other @ _ => {
                        panic!("Expected ClientConnnect, got: {:?}", other);
                    }
                }
            },
            other @ _ => {
                panic!("Expected Ready, got: {:?}", other);
            }
        }
    }

    #[test]
    fn poll_returns_not_ready_when_reader_would_block() {
        let mut subject = ClientMessageStream::with_next_connection_id(WouldBlockRead, FailProtocol);

        let result = subject.poll();
        assert_eq!(Ok(Async::NotReady), result);
    }

    struct WouldBlockRead;
    impl Read for WouldBlockRead {
        fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
            Err(::std::io::Error::new(::std::io::ErrorKind::WouldBlock, "would_block_err"))
        }
    }

    struct FailProtocol;
    impl ClientProtocol for FailProtocol {
        fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
            unimplemented!()
        }
    }

}




