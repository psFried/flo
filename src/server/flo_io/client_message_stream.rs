use futures::{Poll, Async};
use futures::stream::Stream;
use std::io::Read;
use std::time::{Instant};

use server::engine::api::{self, ClientMessage, ProducerMessage, ConsumerMessage, ConnectionId};
use protocol::{ClientProtocol, ProtocolMessage, ProduceEventHeader, ConsumerStart};
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
    ReadEvent(InProgressEvent),
    Disconnected,
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

    fn disconnect_message(&self) -> ClientMessage {
        ClientMessage::Both(ConsumerMessage::Disconnect(self.connection_id), ProducerMessage::Disconnect(self.connection_id))
    }

    fn requires_read(&self) -> bool {
        if let MessageStreamState::ReadEvent(_) = self.state {
            return true;
        }
        self.filled_bytes == 0
    }

    fn try_read(&mut self) -> Poll<usize, String> {
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
                return Ok(Async::Ready(amount))
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
        enum ProtoResult {
            Done(ProtocolMessage),
            Incomplete
        }

        let proto_message: Result<ProtoResult, String> = {
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
                    trace!("ClientMessageStream for connection: {} Got protocolMessage: {:?}", self.connection_id, proto_message);
                    let nused = self.filled_bytes - remaining.len();
                    self.buffer_pos += nused;
                    self.filled_bytes -= nused;
                    Ok(ProtoResult::Done(proto_message))
                }
                IResult::Incomplete(needed) => {
                    //TODO: in case buffer position is > 0, shift buffer in preparation for next read.
                    trace!("Connection: {} got incomplete message. Need: {:?}", self.connection_id, needed);
                    Ok(ProtoResult::Incomplete)
                }
                IResult::Error(err) => {
                    warn!("Error parsing message from client: {:?}", err);
                    Err(format!("Error parsing: {:?}", err))
                }
            }
        };


        match proto_message {
            Ok(ProtoResult::Done(message)) => {
                if let ProtocolMessage::ProduceEvent(header) = message {
                    trace!("Connection: {} parsed event header: {:?}", self.connection_id, header);
                    self.try_parse_event(Some(header))
                } else {
                    let msg: ClientMessage = to_engine_api_message(message, self.connection_id);
                    Ok(Async::Ready(Some(msg)))
                }
            }
            Ok(ProtoResult::Incomplete) => {
                Ok(Async::NotReady)
            }
            Err(err) => Err(err)
        }
    }

    fn try_parse_event(&mut self, evt_header: Option<ProduceEventHeader>) -> Poll<Option<ClientMessage>, String> {
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
                    let ProduceEventHeader {namespace, parent_id, op_id, data_length} = header;
                    InProgressEvent{
                        event: api::ProduceEvent{
                            message_recv_start: Instant::now(),
                            namespace: namespace,
                            parent_id: parent_id,
                            connection_id: *connection_id,
                            op_id: op_id,
                            event_data: vec![0; data_length as usize],
                        },
                        position: 0
                    }
                }).expect("EventHeader must be Some since state was not ReadEvent")
            }
        };

        trace!("reading event data for in progress event: {:?}", in_progress_event);

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
            *buffer_pos += filled_buffer.len();
            *filled_bytes -= filled_buffer.len();
        }

        if in_progress_event.remaining() == 0 {
            // we were able to read all of the data required for the event
            *state = MessageStreamState::Parsing;
            Ok(Async::Ready(Some(ClientMessage::Producer(ProducerMessage::Produce(in_progress_event.event)))))
        } else {
            //need to wait for more data that is part of this event
            *state = MessageStreamState::ReadEvent(in_progress_event);
            Ok(Async::NotReady)
        }

    }

    fn disconnect(&mut self) -> Poll<Option<ClientMessage>, String> {
        if self.state == MessageStreamState::Disconnected {
            Ok(Async::Ready(None))
        } else {
            self.state = MessageStreamState::Disconnected;
            Ok(Async::Ready(Some(self.disconnect_message())))
        }
    }

}

impl <R: Read, P: ClientProtocol> Stream for ClientMessageStream<R, P> {
    type Item = ClientMessage;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Extra guard in case previous poll returned an error and disconnected
        // otherwise, we might try to read or parse again
        if self.state == MessageStreamState::Disconnected {
            return self.disconnect();
        }

        if self.requires_read() {
            match self.try_read() {
                Ok(Async::Ready(nread)) if nread == 0 => {
                    return self.disconnect();
                }
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }
                Err(err) => {
                    return Err(err);
                }
                _ => {} //Ok and nread > 0
            }
        }

        let parse_result = if self.state.is_read_event() {
            self.try_parse_event(None)
        } else {
            self.try_parse()
        };

        match parse_result {
            Err(err) => {
                warn!("Error parsing data from client. Closing connection: {:?}", err);
                return self.disconnect();
            }
            other @ _ => other
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

    use flo_event::FloEventId;
    use server::engine::api::{ClientMessage, ConsumerMessage, ProducerMessage, ClientAuth, ProduceEvent};
    use protocol::{ClientProtocol, ClientProtocolImpl, ProtocolMessage, ProduceEventHeader};
    use nom::{IResult, Needed, ErrorKind, Err};

    use env_logger;
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

        let mut subject = ClientMessageStream::new(123, reader, ClientProtocolImpl);

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

    #[test]
    fn poll_returns_disconnect_and_then_ok_with_empty_option_when_protocol_returns_error() {
        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, _buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                IResult::Error(ErrorKind::Alpha)
            }
        }

        struct Reader;
        impl Read for Reader {
            fn read(&mut self, _buf: &mut [u8]) -> Result<usize, io::Error> {
                Ok(99)
            }
        }

        let mut subject = ClientMessageStream::new(123, Reader, Proto);

        let disconnect = ClientMessage::Both(ConsumerMessage::Disconnect(123), ProducerMessage::Disconnect(123));
        let expected = Ok(Async::Ready(Some(disconnect)));
        let result = subject.poll();
        assert_eq!(expected, result);

        let result = subject.poll();
        let expected = Ok(Async::Ready(None));
        assert_eq!(expected, result);
    }

    #[test]
    fn poll_returns_client_disconnect_and_then_none_if_0_bytes_are_read_and_buffer_is_empty() {
        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, _buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                IResult::Incomplete(Needed::Size(8))
            }
        }

        struct Reader;
        impl Read for Reader {
            fn read(&mut self, _buf: &mut [u8]) -> Result<usize, io::Error> {
                Ok(0)
            }
        }

        let mut subject = ClientMessageStream::new(123, Reader, Proto);

        let result = subject.poll();

        let disconnect = ClientMessage::Both(ConsumerMessage::Disconnect(123), ProducerMessage::Disconnect(123));
        let expected = Ok(Async::Ready(Some(disconnect)));
        assert_eq!(expected, result);

        let result = subject.poll();
        let expected = Ok(Async::Ready(None));
        assert_eq!(expected, result);
    }

    #[test]
    fn poll_returns_event_after_multiple_reads() {
        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                IResult::Done(&buffer[8..], ProtocolMessage::ProduceEvent(ProduceEventHeader {
                    namespace: "foo".to_owned(),
                    parent_id: None,
                    op_id: 789,
                    data_length: 40
                }))
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
            Async::Ready(Some(ClientMessage::Producer(ProducerMessage::Produce(event)))) => {
                let expected_data = (&input_bytes[8..48]).to_vec();
                assert_eq!(expected_data, event.event_data);
            }
            other @ _ => panic!("Expected Ready, got: {:?}", other)
        }
    }

    #[test]
    fn poll_returns_event_when_read_and_parse_both_succeed() {
        let bytes = b"FLO_PRO\n00009999the event data extra bytes";
        //op_id | data_length | event data  | extra bytes
        let reader = Cursor::new(&bytes[..]);
        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                //           remaining buffer excluded data length
                IResult::Done(&buffer[16..], ProtocolMessage::ProduceEvent(ProduceEventHeader {
                    namespace: "foo".to_owned(),
                    parent_id: None,
                    op_id: 999,
                    data_length: 14
                }))
            }
        }

        let mut subject = ClientMessageStream::new(123, reader, Proto);

        let result = subject.poll().expect("Expected Ok, got Err");

        match result {
            Async::Ready(Some(ClientMessage::Producer(ProducerMessage::Produce(event)))) => {
                let expected_bytes: Vec<u8> = "the event data".into();
                assert_eq!(expected_bytes, event.event_data);
                assert_eq!(999, event.op_id);
            }
            other @ _ => {
                panic!("Expected Produce message, got: {:?}", other);
            }
        }

        // 8 for header, 4 for op_id, 4 for length, + 14 for data
        assert_eq!(30, subject.buffer_pos);
        assert_eq!(" extra bytes".len(), subject.filled_bytes);
    }

    #[test]
    fn poll_returns_api_message_when_read_and_parse_both_succeed() {
        let bytes = b"the input bytes plus some other bytes";
        let reader = Cursor::new(&bytes[..]);

        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
                IResult::Done(&buffer[..14], ProtocolMessage::ClientAuth{
                    namespace: "theNamespace".to_owned(),
                    username: "theUsername".to_owned(),
                    password: "thePassword".to_owned(),
                })
            }
        }
        let mut subject = ClientMessageStream::new(123, reader, Proto);

        let result = subject.poll().expect("Expected Ok, got Err");

        let expected_auth = ClientAuth{
            connection_id: 123,
            namespace: "theNamespace".to_owned(),
            username: "theUsername".to_owned(),
            password: "thePassword".to_owned(),
        };

        match result {
            Async::Ready(message) => {
                match message {
                    Some(ClientMessage::Both(consumer_msg, producer_msg)) => {
                        assert_eq!(ConsumerMessage::ClientAuth(expected_auth.clone()), consumer_msg);
                        assert_eq!(ProducerMessage::ClientAuth(expected_auth.clone()), producer_msg);
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
