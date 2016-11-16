use byteorder::{ByteOrder, BigEndian};
use event::{Event, EventId};
use std::io::{self, Write, ErrorKind, Read};
use std::collections::VecDeque;
use std::mem;
use std::cmp;
use nom::{be_u32, be_u64, IResult, le_u32};


const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

named!{parse_event<Event>,
    chain!(
        event_id: be_u64 ~
        event_data: length_bytes!(be_u32),
        || {
            let mut data = Vec::with_capacity(event_data.len());
            data.extend_from_slice(event_data);
            Event {
                id: event_id,
                data: data,
            }
        }
    )
}

pub struct EventStreamDeserializer<T: Read + Sized> {
    next_event: Option<Event>,
    buffer: Vec<u8>,
    reader: T,
    buffer_byte_count: usize,
}

impl <T: Read + Sized> EventStreamDeserializer<T> {
    pub fn new(reader: T) -> EventStreamDeserializer<T> {
        EventStreamDeserializer::with_buffer_size(reader, DEFAULT_BUFFER_SIZE)
    }

    pub fn with_buffer_size(reader: T, buffer_size: usize) -> EventStreamDeserializer<T> {
        EventStreamDeserializer {
            next_event: None,
            buffer: vec![0; buffer_size],
            reader: reader,
            buffer_byte_count: 0,
        }
    }
}

impl <T: Read + Sized> Iterator for EventStreamDeserializer<T> {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {

        let mut buffer_shift_amt = 0;
        let mut buffer_shift_offset = 0;

        while self.next_event.is_none() {
            let EventStreamDeserializer {
                    ref mut buffer_byte_count,
                    ref mut next_event,
                    ref mut buffer,
                    ref mut reader,
                ..} = *self;

            //first try to parse an event
            let buffer_length = buffer.len();
            println!("Buffer byte count: {}", buffer_byte_count);
            if *buffer_byte_count > 0 {
                match parse_event(buffer) {
                    IResult::Done(remaining, event) => {
                        println!("Read event: {:?}", event);
                        *next_event = Some(event);
                        buffer_shift_amt = remaining.len();
                        *buffer_byte_count -= (buffer_length - remaining.len());
                        break;
                    }
                    IResult::Error(err) => {
                        error!("Error parsing event: {:?}", err);
                        break;
                    }
                    IResult::Incomplete(needed) => {
                        //we need some more bytes. grow the buffer!
                        panic!("Need to grow the buffer, but I haven't written that shit yet. Bytes needed: {:?}", needed);
                    }
                }
            }

            let read_result = {
                reader.read(buffer)
            };
            println!("read result: {:?}", read_result);
            match read_result {
                Ok(0) => {
                    break;
                }
                Ok(byte_count) => {
                    //continue in the loop and try again
                    println!("setting buffer byte count to: {}", byte_count);
                    *buffer_byte_count = byte_count;
                },
                Err(ref err) => {
                    error!("Error reading into buffer: {:?}", err);
                    break;
                }
            }
        }


        //if we've read an event, may need to shift the buffer
        if buffer_shift_amt > 0 {
            let offset = self.buffer.len() - buffer_shift_amt;
            println!("shifting {} bytes by: {} offset", buffer_shift_amt, offset);
            for i in 0..buffer_shift_amt {
                self.buffer.swap(i, i + offset);
            }
        }

        self.next_event.take()
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

