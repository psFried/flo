use flo_event::{FloEventId, FloEvent, OwnedFloEvent};
use std::io::{self, Read};

use nom::{be_u16, be_u32, be_u64};
use byteorder::{ByteOrder, BigEndian};

static ACK_HEADER: &'static [u8; 8] = b"FLO_ACK\n";
static EVENT_HEADER: &'static [u8; 8] = b"FLO_EVT\n";
static ERROR_HEADER: &'static [u8; 8] = b"FLO_ERR\n";

pub const ERROR_INVALID_NAMESPACE: u8 = 15;

// Event Acknowledged
#[derive(Debug, PartialEq, Clone)]
pub struct EventAck {
    pub op_id: u32,
    pub event_id: FloEventId,
}
unsafe impl Send for EventAck {}


// Error message
#[derive(Debug, PartialEq, Clone)]
pub enum ErrorKind {
    InvalidNamespaceGlob,
}
unsafe impl Send for ErrorKind {}

#[derive(Debug, PartialEq, Clone)]
pub struct ErrorMessage {
    pub op_id: u32,
    pub kind: ErrorKind,
    pub description: String,
}

impl ErrorKind {
    pub fn from_u8(byte: u8) -> Result<ErrorKind, u8> {
        match byte {
            ERROR_INVALID_NAMESPACE => Ok(ErrorKind::InvalidNamespaceGlob),
            other => Err(other)
        }
    }

    pub fn u8_value(&self) -> u8 {
        match self {
            &ErrorKind::InvalidNamespaceGlob => ERROR_INVALID_NAMESPACE,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ServerMessage<T: FloEvent> {
    EventPersisted(EventAck),
    Event(T),
    Error(ErrorMessage)
}
unsafe impl <T> Send for ServerMessage<T> where T: FloEvent + Send {}

impl <T> Clone for ServerMessage<T> where T: FloEvent + Clone {
    fn clone(&self) -> Self {
        match self {
            &ServerMessage::Event(ref evt) => ServerMessage::Event(evt.clone()),
            &ServerMessage::EventPersisted(ref ack) => ServerMessage::EventPersisted(ack.clone()),
            &ServerMessage::Error(ref kind) => ServerMessage::Error(kind.clone())
        }
    }
}

named!{pub parse_str<String>,
    map_res!(
        take_until_and_consume!("\n"),
        |res| {
            ::std::str::from_utf8(res).map(|val| val.to_owned())
        }
    )
}

named!{parse_event<OwnedFloEvent>,
    chain!(
        _tag: tag!("FLO_EVT\n") ~
        actor: be_u16 ~
        counter: be_u64 ~
        namespace: parse_str ~
        data: length_bytes!(be_u32),
        || {
            OwnedFloEvent {
                id: FloEventId::new(actor, counter),
                namespace: namespace.to_owned(),
                data: data.to_owned()
            }
        }
    )
}

named!{parse_event_ack<EventAck>,
    chain!(
        _tag: tag!(ACK_HEADER) ~
        op_id: be_u32 ~
        counter: be_u64 ~
        actor_id: be_u16,
        || {
            EventAck {
                op_id: op_id,
                event_id: FloEventId::new(actor_id, counter)
            }
        }
    )
}

named!{parse_error_message<ErrorMessage>,
    chain!(
        _tag: tag!(ERROR_HEADER) ~
        op_id: be_u32 ~
        kind: map_res!(take!(1), |res: &[u8]| {
            ErrorKind::from_u8(res[0])
        }) ~
        description: parse_str,
        || {
            ErrorMessage {
                op_id: op_id,
                kind: kind,
                description: description,
            }
        }
    )
}

named!{pub read_server_message<ServerMessage<OwnedFloEvent>>,
    alt!(
        map!(parse_event, |event| ServerMessage::Event(event)) |
        map!(parse_event_ack, |ack| ServerMessage::EventPersisted(ack)) |
        map!(parse_error_message, |err| ServerMessage::Error(err))
    )
}

pub trait ServerProtocol<E: FloEvent>: Read + Sized {
    fn new(server_message: ServerMessage<E>) -> Self;
    fn is_done(&self) -> bool;
}

#[derive(Debug, PartialEq, Clone)]
enum ReadState {
    Init,
    EventData {
        position: usize,
        len: usize,
    },
    Done,
}

pub struct ServerProtocolImpl<E: FloEvent> {
    message: ServerMessage<E>,
    state: ReadState,
}

impl <E: FloEvent> ServerProtocol<E> for ServerProtocolImpl<E> {
    fn new(server_message: ServerMessage<E>) -> ServerProtocolImpl<E> {
        ServerProtocolImpl {
            message: server_message,
            state: ReadState::Init,
        }
    }

    fn is_done(&self) -> bool {
        self.state == ReadState::Done
    }
}


impl <E: FloEvent> Read for ServerProtocolImpl<E> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let ServerProtocolImpl { ref message, ref mut state, } = *self;

        if *state == ReadState::Done {
            return Ok(0);
        }

        match message {
            &ServerMessage::Error(ref err_message) => {
                trace!("serializing error message: {:?}", err_message);
                &buf[..8].copy_from_slice(&ERROR_HEADER[..]);
                BigEndian::write_u32(&mut buf[8..12], err_message.op_id);
                buf[12] = err_message.kind.u8_value();
                let desc_end_idx = err_message.description.len() + 13;
                &buf[13..desc_end_idx].copy_from_slice(err_message.description.as_bytes());
                buf[desc_end_idx] = b'\n';

                *state = ReadState::Done;
                Ok(desc_end_idx + 1)
            }
            &ServerMessage::EventPersisted(ref ack) => {
                trace!("serializing event ack: {:?}", ack);
                let mut nread = 0;
                &buf[..8].copy_from_slice(&ACK_HEADER[..]);
                nread += 8;
                BigEndian::write_u32(&mut buf[8..12], ack.op_id);
                nread += 4;
                BigEndian::write_u64(&mut buf[12..20], ack.event_id.event_counter);
                nread += 8;
                BigEndian::write_u16(&mut buf[20..22], ack.event_id.actor);
                nread += 2;

                *state = ReadState::Done;
                Ok(nread)
            }
            &ServerMessage::Event(ref event) => {
                let (buffer_pos, data_pos): (usize, usize) = match *state {
                    ReadState::Init => {
                        trace!("Writing header for event: {:?}", event);
                        let mut pos: usize = 8;
                        //only write this stuff if we haven't already
                        &buf[..pos].copy_from_slice(&EVENT_HEADER[..]);

                        BigEndian::write_u16(&mut buf[8..10], event.id().actor);
                        BigEndian::write_u64(&mut buf[10..18], event.id().event_counter);
                        pos += 10;

                        let ns_length = event.namespace().len();
                        &buf[pos..(ns_length + pos)].copy_from_slice(event.namespace().as_bytes());
                        pos += ns_length;
                        buf[pos] = 10u8;
                        pos += 1;

                        BigEndian::write_u32(&mut buf[pos..], event.data_len());
                        (pos + 4, 0)
                    }
                    ReadState::EventData{ref position, ..} => (0, *position),
                    ReadState::Done => {
                        return Ok(0);
                    }
                };

                //write what we can of the event data, but the buffer might not have room for it
                let buffer_space = buf.len() - buffer_pos;
                let copy_amount = ::std::cmp::min(event.data_len() as usize - data_pos, buffer_space);
                &buf[buffer_pos..(buffer_pos + copy_amount)].copy_from_slice(&event.data()[data_pos..(data_pos + copy_amount)]);

                let data_len = event.data_len() as usize;
                let new_position = copy_amount + data_pos;
                if data_len == new_position {
                    *state = ReadState::Done;
                } else {
                    *state = ReadState::EventData{position: new_position, len: data_len};
                }

                Ok(buffer_pos + copy_amount)
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use nom::IResult;
    use std::io::Read;
    use flo_event::{FloEventId, OwnedFloEvent};
    use std::sync::Arc;

    fn assert_message_serializes_and_deserializes(message: ServerMessage<OwnedFloEvent>) {
        let mut buffer = [0; 1024];
        let mut serializer = ServerProtocolImpl::new(message.clone());
        let nread = serializer.read(&mut buffer[..]).expect("failed to read message into buffer");
        println!("Buffer: {:?}", &buffer[..nread]);

        let result = read_server_message(&buffer[..nread]);
        match result {
            IResult::Done(rem, message_result) => {
                assert_eq!(message, message_result);
                assert!(rem.is_empty());
            }
            IResult::Incomplete(need) => {
                panic!("expected Done, got Incomplete: {:?}", need)
            }
            IResult::Error(err) => {
                panic!("Error deserializing event: {:?}", err)
            }
        }
    }

    #[test]
    fn error_message_is_serialized_and_deserialized() {
        let err = ServerMessage::Error(ErrorMessage{
            op_id: 1234,
            kind: ErrorKind::InvalidNamespaceGlob,
            description: "Cannot have move than two '*' characters sequentially".to_owned(),
        });
        assert_message_serializes_and_deserializes(err);
    }

    #[test]
    fn event_is_serialized_and_deserialized() {
        let event = ServerMessage::Event(OwnedFloEvent{
            id: FloEventId::new(1, 6),
            namespace: "/the/event/namespace".to_owned(),
            data: "the event data".as_bytes().to_owned(),
        });
        assert_message_serializes_and_deserializes(event);
    }

    #[test]
    fn event_is_written_in_one_pass() {
        let mut subject = ServerProtocolImpl::new(ServerMessage::Event(
            Arc::new(OwnedFloEvent::new(FloEventId::new(12, 23), "the namespace".to_owned(), vec![9; 64]))
        ));

        let mut buffer = [0; 256];

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(100, result);

        let expected_header = b"FLO_EVT\n";
        assert_eq!(&expected_header[..], &buffer[..8]);                //starts with header
        assert_eq!(&[0, 12, 0, 0, 0, 0, 0, 0, 0, 23], &buffer[8..18]); //event id is actor id then event counter
        let expected_namespace = b"the namespace\n";
        assert_eq!(&expected_namespace[..], &buffer[18..32]);   // namespace written with terminating newline
        assert_eq!(&[0, 0, 0, 64], &buffer[32..36]);            //data length as big endian u32

        let expected_data = vec![9; 64];
        assert_eq!(&expected_data[..], &buffer[36..100]);

        assert!(subject.is_done());

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(0, result);
    }

    #[test]
    fn event_is_written_in_multiple_passes() {
        let mut subject = ServerProtocolImpl::new(ServerMessage::Event(
            Arc::new(OwnedFloEvent::new(FloEventId::new(12, 23), "the namespace".to_owned(), vec![9; 64]))
        ));

        let mut buffer = [0; 48];

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(48, result);

        let expected_header = b"FLO_EVT\n";
        assert_eq!(&expected_header[..], &buffer[..8]);                //starts with header
        assert_eq!(&[0, 12, 0, 0, 0, 0, 0, 0, 0, 23], &buffer[8..18]); //event id is actor id then event counter
        let expected_namespace = b"the namespace\n";
        assert_eq!(&expected_namespace[..], &buffer[18..32]);   // namespace written with terminating newline
        assert_eq!(&[0, 0, 0, 64], &buffer[32..36]);            //data length as big endian u32
        assert_eq!(&[9; 12], &buffer[36..]);                    //partial event data

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(48, result);

        let expected_data = vec![9; 48];
        assert_eq!(&expected_data[..], &buffer[..]);

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(4, result);

        assert!(subject.is_done());
        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(0, result);
        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(0, result);
    }

    #[test]
    fn event_ack_is_serialized_and_deserialized() {
        let ack = ServerMessage::EventPersisted(
            EventAck{op_id: 123, event_id: FloEventId::new(234, 5)}
        );
        assert_message_serializes_and_deserializes(ack);
    }
}





