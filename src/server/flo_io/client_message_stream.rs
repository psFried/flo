use futures::{Poll, Async};
use futures::stream::Stream;
use std::io::Read;

use server::engine::api::{self, ClientMessage, ConnectionId};
use protocol::{ClientProtocol, ProtocolMessage, EventHeader};
use nom::IResult;

const BUFFER_SIZE: usize = 8 * 1024;


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

impl MessageStreamState {
    fn is_read_event(&self) -> bool {
        match self {
            &MessageStreamState::ReadEvent(_) => true,
            _ => false
        }
    }
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
    pub fn new(connection_id: ConnectionId, reader: R, protocol: P) -> ClientMessageStream<R, P> {
        ClientMessageStream {
            connection_id: connection_id,
            tcp_reader: reader,
            protocol: protocol,
            buffer: vec![0; BUFFER_SIZE],
            buffer_pos: 0,
            filled_bytes: 0,
            state: MessageStreamState::Reading,
        }
    }

    fn requires_read(&self) -> bool {
        if let MessageStreamState::ReadEvent(_) = self.state {
            return true;
        }
        self.filled_bytes == 0
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
                *filled_bytes = amount;
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
        let proto_message = {
            let parse_result = {
                let ClientMessageStream {
                    ref mut buffer,
                    ref mut buffer_pos,
                    ref mut filled_bytes,
                    ref protocol,
                    ..
                } = *self;

                protocol.parse_any(&buffer[*buffer_pos..(*buffer_pos + *filled_bytes)])
            };

            match parse_result {
                IResult::Done(remaining, proto_message) => {
                    let nused = self.filled_bytes - remaining.len();
                    self.buffer_pos += nused;
                    self.filled_bytes -= nused;
                    Ok(proto_message)
                }
                IResult::Incomplete(needed) => {
                    //TODO: in case buffer position is > 0, shift buffer in preparation for next read.
                    Err(format!("Need to write handling of incomplete parse. Needed: {:?}", needed))
                }
                IResult::Error(err) => {
                    Err(format!("Error parsing: {:?}", err))
                }
            }
        };

        match proto_message {
            Ok(ProtocolMessage::ApiMessage(msg)) => {
                Ok(Async::Ready(Some(msg)))
            }
            Ok(ProtocolMessage::ProduceEvent(evt_header)) => {
                self.try_parse_event(Some(evt_header))
            }
            Err(err) => Err(err)
        }
    }

    fn try_parse_event(&mut self, evt_header: Option<EventHeader>) -> Poll<Option<ClientMessage>, String> {
        let ClientMessageStream{
            ref mut state,
            ref mut buffer,
            ref mut buffer_pos,
            ref mut filled_bytes,
            ref connection_id,
            ..
        } = *self;
        let mut in_progress_event = {
            let prev_state = ::std::mem::replace(state, MessageStreamState::Parsing);
            if let MessageStreamState::ReadEvent(evt) = prev_state {
                evt
            } else {
                evt_header.map(|header| {
                    InProgressEvent{
                        event: api::ProduceEvent{
                            connection_id: *connection_id,
                            op_id: header.op_id,
                            event_data: vec![0; header.data_length as usize]
                        },
                        position: 0
                    }
                }).expect("EventHeader must be Some sinc state was not ReadEvent")
            }
        };

        //copy available data into event
        {
            let evt_current_pos = in_progress_event.position;
            let evt_remaining = in_progress_event.remaining();

            let src_end = ::std::cmp::min(*filled_bytes, evt_remaining) + *buffer_pos;
            let filled_buffer = &buffer[*buffer_pos..src_end];
            in_progress_event.position += filled_buffer.len();
            let dst_end = filled_buffer.len() + evt_current_pos;
            let mut dst = &mut in_progress_event.event.event_data[evt_current_pos..dst_end];
            dst.copy_from_slice(filled_buffer);
        }

        if in_progress_event.remaining() == 0 {
            // we were able to read all of the data required for the event
            *state = MessageStreamState::Parsing;
            Ok(Async::Ready(Some(ClientMessage::Produce(in_progress_event.event))))
        } else {
            //need to wait for more data that is part of this event
            *state = MessageStreamState::ReadEvent(in_progress_event);
            Ok(Async::NotReady)
        }

    }

}

impl <R: Read, P: ClientProtocol> Stream for ClientMessageStream<R, P> {
    type Item = ClientMessage;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.requires_read() {
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

        if self.state.is_read_event() {
            self.try_parse_event(None)
        } else {
            self.try_parse()
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use futures::Async;
    use futures::stream::Stream;
    use std::io::{Read, Cursor};

    use server::engine::api::{ClientMessage, ClientAuth};
    use protocol::{ClientProtocol, ProtocolMessage, EventHeader};
    use nom::IResult;

    #[test]
    fn poll_returns_event_after_multiple_reads() {
        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                IResult::Done(&buffer[8..], ProtocolMessage::ProduceEvent(EventHeader{op_id: 789, data_length: 40}))
            }
        }

        struct Reader(Vec<u8>, usize);
        impl Read for Reader {
            fn read(&mut self, buf: &mut [u8]) -> Result<usize, ::std::io::Error> {
                let start = self.1;
                let amount = 16;
                let end = self.1 + amount;
                &buf[..amount].copy_from_slice(&self.0[start..end]);
                self.1 += amount;
                Ok(amount)
            }
        }

        let input_bytes = b"00000000the event data is a little bit longer and will be consumed in three reads";
        let reader = Reader(input_bytes.to_vec(), 0);

        let mut subject = ClientMessageStream::new(123, reader, Proto);

        let result = subject.poll().expect("Expected Ok, got Err");
        assert_eq!(Async::NotReady, result);
        let result = subject.poll().expect("Expected Ok, got Err");
        assert_eq!(Async::NotReady, result);

        let result = subject.poll().expect("Expected Ok, got Err");
        match result {
            Async::Ready(Some(ClientMessage::Produce(event))) => {
                let expected_data = (&input_bytes[8..48]).to_vec();
                assert_eq!(expected_data, event.event_data);
            }
            other @ _ => panic!("Expected Ready, got: {:?}", other)
        }
    }

    #[test]
    fn poll_returns_event_when_read_and_parse_both_succeed() {
        let bytes = b"00000000the event data extra bytes";
        //op_id | data_length | event data  | extra bytes
        let reader = Cursor::new(&bytes[..]);
        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                //           remaining buffer excluded data length
                IResult::Done(&buffer[8..], ProtocolMessage::ProduceEvent(EventHeader{op_id: 999, data_length: 14}))
            }
        }

        let mut subject = ClientMessageStream::new(123, reader, Proto);

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
        let mut subject = ClientMessageStream::new(123, reader, Proto);

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
        let mut subject = ClientMessageStream::new(123, WouldBlockRead, FailProtocol);

        let result = subject.poll();
        assert_eq!(Ok(Async::NotReady), result);
    }

    struct WouldBlockRead;
    impl Read for WouldBlockRead {
        fn read(&mut self, _buf: &mut [u8]) -> ::std::io::Result<usize> {
            Err(::std::io::Error::new(::std::io::ErrorKind::WouldBlock, "would_block_err"))
        }
    }

    struct FailProtocol;
    impl ClientProtocol for FailProtocol {
        fn parse_any<'a>(&'a self, _buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
            unimplemented!()
        }
    }

}
