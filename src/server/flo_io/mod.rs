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

#[derive(Debug, PartialEq)]
pub struct InProgressEvent {
    event: api::ProduceEvent,
    position: usize,
}

impl InProgressEvent {
    fn remaining(&self) -> usize {
        self.event.event_data.len() - self.position
    }
}

#[derive(Debug, PartialEq)]
enum MessageStreamState {
    Reading,
    Parsing,
    ReadEvent(InProgressEvent)
}

pub struct ClientMessageStream<R: Read, P: ClientProtocol> {
    connection_id: ConnectionId,
    tcp_reader: R,
    protocol: P,
    buffer: Vec<u8>,
    buffer_pos: usize,
    filled_bytes: usize,
    state: MessageStreamState,
}

impl <R: Read, P: ClientProtocol> ClientMessageStream<R, P> {
    pub fn with_next_connection_id(reader: R, protocol: P) -> ClientMessageStream<R, P> {
        let conn_id = api::next_connection_id();
        let buffer = vec![0; 8 * 1024];
        ClientMessageStream {
            connection_id: conn_id,
            tcp_reader: reader,
            protocol: protocol,
            buffer: buffer,
            buffer_pos: 0,
            filled_bytes: 0,
            state: MessageStreamState::Reading,
        }
    }

    fn try_read(&mut self) -> Poll<(), String> {
        let ClientMessageStream{ref mut tcp_reader,
                ref mut buffer,
                ref mut buffer_pos,
                ref mut filled_bytes,
            ..} = *self;
        let read_result = tcp_reader.read(buffer);
        match read_result {
            Ok(amount) => {
                // Reset the buffer position after successful read
                *buffer_pos = 0;
                *filled_bytes += amount;
                return Ok(Async::Ready(()))
            },
            Err(ref err) if err.kind() == ::std::io::ErrorKind::WouldBlock => {
                return Ok(Async::NotReady);
            },
            Err(err) => {
                return Err(format!("Error reading from stream: {:?}", err));
            }
        }
    }

    fn try_parse(&mut self) -> Poll<Option<ClientMessage>, String> {
        let ClientMessageStream {
            ref connection_id,
            ref mut tcp_reader,
            ref mut buffer,
            ref mut buffer_pos,
            ref mut filled_bytes,
            ref protocol,
            ref mut state,
        } = *self;

        let buffer_len = *filled_bytes - *buffer_pos;

        match self.protocol.parse_any(&buffer[*buffer_pos..(*buffer_pos + *filled_bytes)]) {
            IResult::Done(remaining, proto_message) => {
                //TODO: update buffer_position and filled_bytes

                let nused = buffer_len - remaining.len();
                *buffer_pos += nused;
                *filled_bytes -= nused;
                match proto_message {
                    ProtocolMessage::ApiMessage(msg) => {
                        Ok(Async::Ready(Some(msg)))
                    }
                    ProtocolMessage::ProduceEvent{op_id, data_length} => {
                        let mut event_data = vec![0; data_length as usize];
                        let remaining_len = remaining.len();
                        let data_available = ::std::cmp::min(data_length as usize, remaining_len);
                        for i in 0..data_available {
                            event_data[i] = remaining[i];
                        }
                        // make sure position is correct in case we need to do another parse off of this same buffer
                        *buffer_pos += data_available;

                        let event = api::ProduceEvent {
                            connection_id: self.connection_id,
                            op_id: op_id,
                            event_data: event_data,
                        };
                        if data_available == data_length as usize {
                            // we were able to read all of the data required for the event
                            Ok(Async::Ready(Some(ClientMessage::Produce(event))))
                        } else {
                            //need to wait for more data that is part of this event
                            *state = MessageStreamState::Parsing;
                            Ok(Async::NotReady)
                        }
                    }
                }
            }
            IResult::Incomplete(needed) => {
                //TODO: in case buffer position is > 0, shift buffer in preparation for next read.
                panic!("Need to write handling of incomplete parse");
            }
            IResult::Error(err) => {
                Err(format!("Error parsing: {:?}", err))
            }
        }
    }
}

impl <R: Read, P: ClientProtocol> Stream for ClientMessageStream<R, P> {
    type Item = ClientMessage;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {

        if self.filled_bytes == 0 {
            match self.try_read() {
                Ok(Async::Ready(_)) => {

                }
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        self.try_parse()
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
    fn poll_returns_event_when_read_and_parse_both_succeed() {
        let bytes = b"00000000the event data extra bytes";
        //op_id | data_length | event data  | extra bytes
        let reader = Cursor::new(&bytes[..]);
        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                //           remaining buffer excluded data length
                IResult::Done(&buffer[8..], ProtocolMessage::ProduceEvent{op_id: 999, data_length: 14})
            }
        }

        let mut subject = ClientMessageStream::with_next_connection_id(reader, Proto);

        let result = subject.poll().expect("Expected Ok, got Err");

        match result {
            Async::Ready(Some(ClientMessage::Produce(event))) => {
                let expected_bytes: Vec<u8> = "the event data".into();
                assert_eq!(expected_bytes, event.event_data);
                assert_eq!(999, event.op_id);
            }
            other @ _ => {
                panic!("Expected Produce message, got: {:?}", other);
            }
        }

    }

    #[test]
    fn poll_returns_api_message_when_read_and_parse_both_succeed() {
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




