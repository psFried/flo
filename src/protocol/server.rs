use flo_event::{FloEventId, FloEvent, OwnedFloEvent};
use std::io::{self, Read, Write};
use std::sync::Arc;

use byteorder::{ByteOrder, BigEndian};

#[derive(Debug, PartialEq)]
pub struct EventAck {
    pub op_id: u32,
    pub event_id: FloEventId,
}
unsafe impl Send for EventAck {}

#[derive(Debug, PartialEq)]
pub enum ServerMessage<T: FloEvent> {
    EventPersisted(EventAck),
    Event(T),
}
unsafe impl <T> Send for ServerMessage<T> where T: FloEvent + Send {}

pub trait ServerProtocol: Read + Sized {
    fn new(server_message: ServerMessage<Arc<OwnedFloEvent>>) -> Self;
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
    message: ServerMessage<Arc<OwnedFloEvent>>,
    state: ReadState,
}

impl ServerProtocol for ServerProtocolImpl {
    fn new(server_message: ServerMessage<Arc<OwnedFloEvent>>) -> ServerProtocolImpl {
        ServerProtocolImpl {
            message: server_message,
            state: ReadState::Init,
        }
    }

    fn is_done(&self) -> bool {
        self.state == ReadState::Done
    }
}

static ACK_HEADER: &'static [u8; 8] = b"FLO_ACK\n";
static EVENT_HEADER: &'static [u8; 8] = b"FLO_EVT\n";

impl Read for ServerProtocolImpl {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let ServerProtocolImpl { ref message, ref mut state, } = *self;

        if *state == ReadState::Done {
            return Ok(0);
        }

        match message {
            &ServerMessage::EventPersisted(ref ack) => {
                trace!("serializing event ack: {:?}", ack);
                let mut nread = 0;
                &buf[..8].copy_from_slice(&ACK_HEADER[..]);
                nread += 8;
                BigEndian::write_u32(&mut buf[8..12], ack.op_id);
                nread += 4;
                BigEndian::write_u16(&mut buf[12..14], ack.event_id.actor);
                nread += 2;
                BigEndian::write_u64(&mut buf[14..22], ack.event_id.event_counter);
                nread += 8;

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

                        BigEndian::write_u16(&mut buf[8..10], event.id.actor);
                        BigEndian::write_u64(&mut buf[10..18], event.id.event_counter);
                        pos += 10;

                        let ns_length = event.namespace.len();
                        &buf[pos..(ns_length + pos)].copy_from_slice(event.namespace.as_bytes());
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
    use std::io::Read;
    use flo_event::{FloEventId, OwnedFloEvent};
    use std::sync::Arc;

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
    fn event_ack_is_written_in_one_go() {
        let mut subject = ServerProtocolImpl::new(ServerMessage::EventPersisted(
            EventAck{op_id: 123, event_id: FloEventId::new(234, 5)}
        ));

        let mut buffer = [0; 64];
        let result = subject.read(&mut buffer[..]).unwrap();

        let expected_header = b"FLO_ACK\n";
        assert_eq!(&expected_header[..], &buffer[..8]);
        assert_eq!(&[0, 0, 0, 123], &buffer[8..12]); // op_id
        assert_eq!(&[0, 234], &buffer[12..14]);
        assert_eq!(&[0, 0, 0, 0, 0, 0, 0, 5], &buffer[14..22]);

        assert_eq!(22, result);

        assert!(subject.is_done());
        let result = subject.read(&mut buffer[..]).unwrap();
        assert_eq!(0, result);
    }
}





