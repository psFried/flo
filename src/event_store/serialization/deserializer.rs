use byteorder::BigEndian;
use event::Event;
use std::io::{ErrorKind, Read};
use std::mem;
use nom::{be_u32, be_u64, IResult};


const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

pub struct EventDeserializer<T: Read> {
    reader: T,
    buffer_1: Vec<u8>,
    buffer_2: Vec<u8>,
    start_position: usize,
}

impl <T: Read> EventDeserializer<T> {
    pub fn new(reader: T) -> EventDeserializer<T> {

        EventDeserializer {
            reader: reader,
            buffer_1: vec![0; DEFAULT_BUFFER_SIZE],
            buffer_2: vec![0; DEFAULT_BUFFER_SIZE],
            start_position: 0,
        }
    }

    fn parse_next_event(&mut self, buffer_end: usize) -> Option<Event> {
        let return_val = {
            let EventDeserializer { ref mut buffer_1, ref mut buffer_2, mut start_position, .. } = *self;
            match parse_event(&buffer_1[..buffer_end]) {
                IResult::Done(remaining, event) => {
                    for i in 0..remaining.len() {
                        buffer_2[i] = remaining[i];
                    }
                    start_position = remaining.len();
                    Some(event)
                }
                IResult::Error(err) => {
                    error!("Error parsing event: {:?}", err);
                    None
                }
                IResult::Incomplete(needed) => {
                    panic!("implement handling of incomplete");
                }
            }
        };

        if return_val.is_some() {
            //swap the buffers so we can start off with a fresh one
            let EventDeserializer { ref mut buffer_1, ref mut buffer_2, .. } = *self;
            mem::swap(buffer_1, buffer_2);
        }
        return_val
    }
}

named!{parse_event<Event>,
    chain!(
        event_id: be_u64 ~
        data: length_bytes!(be_u32),
        || {
            let mut d = Vec::with_capacity(data.len());
            d.extend_from_slice(data);
            Event::new(event_id, d)
        }
    )
}

named!{parse_events<Event>, many0!(parse_event)}

impl <T: Read> Iterator for EventDeserializer<T> {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {

        if self.start_position == 0 {
            let read_result = {
                let EventDeserializer {ref mut reader, ref mut buffer_1, start_position, ..} = *self;
                reader.read(&mut buffer_1[start_position..])
            };

            match read_result {
                Ok(0) => None,
                Ok(byte_count) => {
                    self.parse_next_event(byte_count)
                },
                Err(ref err) => {
                    error!("Error reading into buffer: {:?}", err);
                    None
                }
            }
        } else {
            let length = self.buffer_1.len();
            self.parse_next_event(length)
        }

    }
}

#[test]
fn parser_parses_a_single_event() {
    use std::io::Cursor;

    let bytes = [
        0, 0, 0, 0, 0, 0, 0, 5, // event id
        0, 0, 0, 7,             // data length
        1, 2, 3, 4, 5, 6, 7,    // data
        8, 9, 10, 11            // extra data
    ];

    let (unused, result) = parse_event(&bytes).unwrap();

    let expected_event = Event {
        id: 5,
        data: vec![1, 2, 3, 4, 5, 6, 7]
    };

    assert_eq!(expected_event, result);
    assert_eq!(&bytes[19..], unused);
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

    let mut deserializer = EventDeserializer::new(Cursor::new(&bytes));

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

