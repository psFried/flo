
use std::io::{self, Read, Cursor};
use event::{EventId, Event};

use byteorder::{ByteOrder, BigEndian};

enum SerializePosition {
    Id,
    DataLength,
    Data(usize)
}

pub struct EventSerializer<'a> {
    event_id: EventId,
    cursor: Cursor<&'a [u8]>,
    position: SerializePosition,
}

impl <'a> EventSerializer<'a> {
    pub fn new(event: &'a Event) -> EventSerializer<'a> {
        EventSerializer {
            event_id: event.id,
            cursor: Cursor::new(&event.data),
            position: SerializePosition::Id,
        }
    }
}

const U64_AS_BYTES_LENGTH: usize = 8;

impl <'a> Read for EventSerializer<'a> {
    // TODO: Read should be able to leave off in the middle of writing the event id or data length
    fn read(&mut self, mut buffer: &mut [u8]) -> io::Result<usize> {
        BigEndian::write_u64(buffer, self.event_id);
        let mut buffer = &mut buffer[U64_AS_BYTES_LENGTH ..];
        BigEndian::write_u64(buffer, self.cursor.get_ref().len() as u64);

        let mut buffer = &mut buffer[U64_AS_BYTES_LENGTH ..];

        let byte_count = 2 * U64_AS_BYTES_LENGTH;
        let free_space = buffer.len();

        self.cursor.read(&mut buffer).map(|count| count + (2 * U64_AS_BYTES_LENGTH))
    }
}

pub struct EventDeserializer {
    event: Option<Event>,
    position: SerializePosition,
}


#[cfg(test)]
mod test {
    use super::*;
    use event::Event;
    use std::io::Read;
    use std::io::Cursor;

    #[test]
    fn event_is_serialized() {
        let event = Event::new(1, vec![1, 2, 3]);
        let mut serializer = EventSerializer::new(&event);

        let mut buffer = [0; 20];

        let result = serializer.read(&mut buffer);

        let byte_count = result.unwrap();
        assert_eq!(19, byte_count);

        let expected_bytes = [
            0, 0, 0, 0, 0, 0, 0, 1, //event id
            0, 0, 0, 0, 0, 0, 0, 3, //length of data section
            1, 2, 3,                //data section
            0                       //unused portion of buffer
        ];
        assert_eq!(expected_bytes, buffer);
    }
}
