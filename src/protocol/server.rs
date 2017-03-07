use event::{FloEvent, OwnedFloEvent};
use std::io::{self, Read};

use protocol::ProtocolMessage;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub enum ServerMessage {
    Event(Arc<OwnedFloEvent>),
    Other(ProtocolMessage)
}

impl Clone for ServerMessage {
    fn clone(&self) -> Self {
        match self {
            &ServerMessage::Event(ref evt) => ServerMessage::Event(evt.clone()),
            &ServerMessage::Other(ref protocol_msg) => ServerMessage::Other((*protocol_msg).clone())
        }
    }
}


pub trait ServerProtocol: Read + Sized {
    fn new(server_message: ServerMessage) -> Self;
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

pub struct ServerProtocolImpl {
    message: ServerMessage,
    state: ReadState,
}

impl ServerProtocol for ServerProtocolImpl {
    fn new(server_message: ServerMessage) -> ServerProtocolImpl {
        ServerProtocolImpl {
            message: server_message,
            state: ReadState::Init,
        }
    }

    fn is_done(&self) -> bool {
        self.state == ReadState::Done
    }
}


//TODO: We really shouldn't need a ServerProtocol at all. Should just make the ProtocolMessage and MessageWriter both generic over the type of event (OwnedFloEvent vs. Arc<OwnedFloEvent>)
impl Read for ServerProtocolImpl {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use serializer::Serializer;
        let ServerProtocolImpl { ref message, ref mut state, } = *self;

        if *state == ReadState::Done {
            return Ok(0);
        }

        match message {
            &ServerMessage::Other(ref protocol_message) => {
                trace!("serializing protocol message: {:?}", protocol_message);
                let nbytes = protocol_message.serialize(buf);
                *state = ReadState::Done;
                Ok(nbytes)
            }
            &ServerMessage::Event(ref event) => {
                let (buffer_pos, data_pos): (usize, usize) = match *state {
                    ReadState::Init => {
                        trace!("Writing header for event: {:?}", event);

                        let header_len = Serializer::new(buf).write_bytes("FLO_EVT\n")
                                .write_u64(event.id.event_counter)
                                .write_u16(event.id.actor)
                                .write_u64(event.parent_id.map(|id| id.event_counter).unwrap_or(0))
                                .write_u16(event.parent_id.map(|id| id.actor).unwrap_or(0))
                                .write_u64(::time::millis_since_epoch(event.timestamp))
                                .newline_term_string(&event.namespace)
                                .write_u32(event.data.len() as u32)
                                .finish();
                        (header_len, 0)
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
    use std::io::{Read, Cursor};
    use event::{FloEventId, OwnedFloEvent};
    use std::sync::Arc;
    use byteorder::{ByteOrder, BigEndian};
    use protocol::{ProtocolMessage, MessageStream, ErrorMessage, ErrorKind, EventAck};

    fn assert_event_serializes_and_deserializes(event: OwnedFloEvent) {
        let mut buffer = [0; 1024];
        let mut serializer = ServerProtocolImpl::new(ServerMessage::Event(Arc::new(event.clone())));
        let nread = serializer.read(&mut buffer[..]).expect("failed to read message into buffer");
        println!("Buffer: {:?}", &buffer[..nread]);

        let mut message_stream = MessageStream::new(Cursor::new(&buffer[..nread]));
        let result = message_stream.read_next().expect("Failed to deserialize message");
        match result {
            ProtocolMessage::ReceiveEvent(received) => {
                assert_eq!(event, received);
            }
            other @ _ => panic!("Expected to read an event but got: {:?}", other)
        }
    }

    fn assert_message_serializes_and_deserializes(message: ProtocolMessage) {
        let mut buffer = [0; 1024];
        let mut serializer = ServerProtocolImpl::new(ServerMessage::Other(message.clone()));
        let nread = serializer.read(&mut buffer[..]).expect("failed to read message into buffer");
        println!("Buffer: {:?}", &buffer[..nread]);

        let mut message_stream = MessageStream::new(Cursor::new(&buffer[..nread]));
        let result = message_stream.read_next().expect("Failed to deserialize message");
        assert_eq!(message, result);
    }

    #[test]
    fn error_message_is_serialized_and_deserialized() {
        let err = ProtocolMessage::Error(ErrorMessage{
            op_id: 1234,
            kind: ErrorKind::InvalidNamespaceGlob,
            description: "Cannot have move than two '*' characters sequentially".to_owned(),
        });
        assert_message_serializes_and_deserializes(err);
    }

    #[test]
    fn event_is_serialized_and_deserialized_as_header_and_data() {
        let namespace = "/the/event/namespace";
        let event_data = "the event data";
        let event_id = FloEventId::new(1, 6);
        let parent_id = Some(FloEventId::new(123, 456));
        //time needs to be from milliseconds, otherwise we lose precision
        let event_ts = ::time::from_millis_since_epoch(1);

        let event = OwnedFloEvent{
            id: event_id,
            parent_id: parent_id,
            timestamp: event_ts,
            namespace: namespace.to_owned(),
            data: event_data.as_bytes().to_owned(),
        };
        assert_event_serializes_and_deserializes(event);
    }

    #[test]
    fn event_is_written_in_one_pass() {
        let mut subject = ServerProtocolImpl::new(ServerMessage::Event(
            Arc::new(OwnedFloEvent::new(FloEventId::new(12, 23), None, ::time::from_millis_since_epoch(2), "the namespace".to_owned(), vec![9; 64]))
        ));

        let mut buffer = [0; 256];

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(118, result);

        let expected_header = b"FLO_EVT\n";
        assert_eq!(&expected_header[..], &buffer[..8]);                //starts with header
        assert_eq!(&[0, 0, 0, 0, 0, 0, 0, 23, 0, 12], &buffer[8..18]); //event id is event counter then actor
        assert_eq!(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0], &buffer[18..28]);  //parent event id is event counter then actor
        let expected_namespace = b"the namespace\n";
        assert_eq!(&expected_namespace[..], &buffer[36..50]);   // namespace written with terminating newline
        assert_eq!(&[0, 0, 0, 64], &buffer[50..54]);            //data length as big endian u32

        let expected_data = vec![9; 64];
        assert_eq!(&expected_data[..], &buffer[54..118]);

        assert!(subject.is_done());

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(0, result);
    }

    #[test]
    fn event_is_written_in_multiple_passes() {
        let timestamp_millis = 12345;
        let timestamp = ::time::from_millis_since_epoch(timestamp_millis);
        let mut subject = ServerProtocolImpl::new(ServerMessage::Event(
            Arc::new(OwnedFloEvent::new(FloEventId::new(12, 23), None, timestamp, "the namespace".to_owned(), vec![9; 64]))
        ));

        let mut buffer = [0; 64];

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(64, result);

        let expected_header = b"FLO_EVT\n";
        assert_eq!(&expected_header[..], &buffer[..8]);                //starts with header
        assert_eq!(&[0, 0, 0, 0, 0, 0, 0, 23, 0, 12], &buffer[8..18]); //event id is counter then actor
        assert_eq!(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0], &buffer[18..28]);  // parent event id is counter then actor

        let mut expected_timestamp = [0; 8];
        BigEndian::write_u64(&mut expected_timestamp[..], timestamp_millis);
        assert_eq!(&expected_timestamp[..], &buffer[28..36]);
        //TODO: assert timestamp is correct
        let expected_namespace = b"the namespace\n";
        assert_eq!(&expected_namespace[..], &buffer[36..50]);   // namespace written with terminating newline
        assert_eq!(&[0, 0, 0, 64], &buffer[50..54]);            //data length as big endian u32
        assert_eq!(&[9; 10], &buffer[54..]);                    //partial event data

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(54, result);

        let expected_data = vec![9; 54];
        assert_eq!(&expected_data[..], &buffer[..54]);
        assert!(subject.is_done());

        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(0, result);
        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(0, result);
    }

    #[test]
    fn event_ack_is_serialized_and_deserialized() {
        let ack = ProtocolMessage::AckEvent(
            EventAck{op_id: 123, event_id: FloEventId::new(234, 5)}
        );
        assert_message_serializes_and_deserializes(ack);
    }
}





