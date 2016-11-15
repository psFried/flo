use byteorder::{ByteOrder, BigEndian};
use event::{Event, EventId};
use std::io::{self, Write, ErrorKind, Read};
use std::collections::VecDeque;
use std::mem;
use std::cmp;
use nom::{be_u32, be_u64, IResult};


const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

#[derive(Debug, PartialEq)]
enum DeserializerState {
    Id([u8; 8], usize),
    Length(EventId, [u8; 4], usize),
    Data(EventId, u32, Vec<u8>)
}

impl DeserializerState {
    fn new() -> DeserializerState {
        DeserializerState::Id([0; 8], 0)
    }

    fn is_complete(&self) -> bool {
        match self {
            &DeserializerState::Data(_, data_length, ref data) => {
                data_length as usize == data.len()
            }
            _ => false
        }
    }

    fn push_bytes(self, bytes: &[u8]) -> (usize, DeserializerState) {
        let mut used = 0;
        let mut ret = self;

        if let DeserializerState::Id(mut id_bytes, mut written) = ret {
            let write_count = DeserializerState::write_bytes(bytes, used, &mut id_bytes, written, 8);
            used += write_count;
            written += write_count;

            if written == 8 {
                let event_id = BigEndian::read_u64(&id_bytes);
                ret = DeserializerState::Length(event_id, [0; 4], 0)
            }
        }

        if let DeserializerState::Length(id, mut length_bytes, mut written) = ret {
            let write_count = DeserializerState::write_bytes(bytes, used, &mut length_bytes, written, 4);
            used += write_count;
            written += write_count;

            if written == 4 {
                let data_length = BigEndian::read_u32(&length_bytes);
                ret = DeserializerState::Data(id, data_length, vec![0; data_length as usize]);
            }
        }

        if let DeserializerState::Data(id, data_length, mut data) = ret {
            let already_written = data_length as usize - data.len();
            let write_count = DeserializerState::write_bytes(bytes, used, &mut data, already_written, data_length as usize);
            used += write_count;
            ret = DeserializerState::Data(id, data_length, data);
        }

        (used, ret)
    }

    fn write_bytes(src: &[u8], already_used: usize, dest: &mut [u8], already_written: usize, max: usize) -> usize {
        let src_end = cmp::min(already_used + max, src.len());

        let to_copy = src_end - already_used;
        let dest_end = already_written + to_copy;

        dest[already_written .. dest_end].copy_from_slice(&src[already_used..src_end]);
        to_copy
    }
}

#[test]
fn deserializer_state_push_bytes_sets_id() {
    use std::u64;

    let mut subject = DeserializerState::new();
    let bytes = [255; 8];
    let (num_consumed, state) = subject.push_bytes(&bytes);

    assert_eq!(8, num_consumed);
    let expected = DeserializerState::Length(u64::MAX, [0; 4], 0);
    assert_eq!(expected, state);
}

#[test]
fn deserializer_state_push_bytes_sets_id_and_length() {
    let bytes = [
        0, 0, 0, 0, 0, 0, 0, 1, //id
        0, 0, 0, 7              //length
    ];
    let mut subject = DeserializerState::new();
    let (num_consumed, state) = subject.push_bytes(&bytes);

    assert_eq!(12, num_consumed);
    let expected = DeserializerState::Data(1, 7, vec![0; 7]);
    assert_eq!(expected, state);
}

#[test]
fn deserializer_state_push_bytes_sets_id_length_and_data() {
    let bytes = [
        0, 0, 0, 0, 0, 0, 0, 1, //id
        0, 0, 0, 7,             //length
        1, 2, 3, 4, 5, 6, 7,    //data
        9, 8, 7, 6, 5           //extra data
    ];
    let mut subject = DeserializerState::new();
    let (num_consumed, state) = subject.push_bytes(&bytes);

    assert_eq!(19, num_consumed);

    let expected = DeserializerState::Data(1, 7, vec![1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(expected, state);
}


pub struct EventDeserializer {
    buffer: Vec<u8>,
    state: DeserializerState,
    event_buffer: VecDeque<Event>,
}

impl EventDeserializer {
    pub fn new() -> EventDeserializer {

        EventDeserializer {
            buffer: vec![0; DEFAULT_BUFFER_SIZE],
            state: DeserializerState::Id([0; 8], 0),
            event_buffer: VecDeque::with_capacity(8),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
    }

    fn has_next_event(&self) -> bool {
        false
    }

    fn get_next_event(&self) -> Option<Event> {
        None
    }

}

pub struct EventStreamDeserializer<T: Read + Sized> {
    deserializer: EventDeserializer,
    buffer: Vec<u8>,
    reader: T,
}

impl <T: Read + Sized> EventStreamDeserializer<T> {
    pub fn new(reader: T) -> EventStreamDeserializer<T> {
        EventStreamDeserializer {
            deserializer: EventDeserializer::new(),
            buffer: vec![0; DEFAULT_BUFFER_SIZE],
            reader: reader,
        }
    }
}

impl <T: Read + Sized> Iterator for EventStreamDeserializer<T> {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {

        while !self.deserializer.has_next_event() {
            let EventStreamDeserializer {ref mut deserializer, ref mut buffer, ref mut reader, ..} = *self;
            let read_result = reader.read(buffer);

            match read_result {
                Ok(0) => {
                    break;
                }
                Ok(byte_count) => {
                    deserializer.push_bytes(&mut buffer[..byte_count]);
                },
                Err(ref err) => {
                    error!("Error reading into buffer: {:?}", err);
                    break;
                }
            }

        }

        self.deserializer.get_next_event()
    }
}

#[test]
fn deserializer_reads_multiple_events() {
    use std::io::Cursor;

    ::env_logger::init();

    let bytes = vec![
        0, 0, 0, 0, 0, 0, 0, 5, // event id
        0, 0, 0, 7,             // data length
        1, 2, 3, 4, 5, 6, 7,    // data
        0, 0, 0, 0, 0, 0, 0, 6, // event id
        0, 0, 0, 3,             // data length
        1, 2, 3,                // data
        8, 9, 10, 11            // extra data
    ];

    let mut deserializer = EventStreamDeserializer::new(Cursor::new(&bytes));

    let result_1 = deserializer.next().expect("expected first event");
    let expected_event_1 = Event {
        id: 5,
        data: vec![1, 2, 3, 4, 5, 6, 7]
    };

    assert_eq!(expected_event_1, result_1);

    let result_2 = deserializer.next().expect("expected second event");
    let expected_event_2 = Event {
        id: 6,
        data: vec![1, 2, 3]
    };

    assert_eq!(expected_event_2, result_2);
}

