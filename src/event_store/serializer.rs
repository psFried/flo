
use std::io::{self, Read, Cursor};
use event::{EventId, Event};

use byteorder::{ByteOrder, BigEndian};

enum SerializePosition {
    Id,
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

impl <'a> Read for EventSerializer<'a> {
    fn read(&mut self, mut buffer: &mut [u8]) -> io::Result<usize> {
        let free_space = buffer.len();
        BigEndian::write_u64(buffer, self.event_id);
        let mut b = &mut buffer[8 ..];
        io::copy(&mut self.cursor, &mut b).map(|count| count as usize + 8)
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

        let mut buffer = [0; 12];

        let result = serializer.read(&mut buffer);

        let byte_count = result.unwrap();
        assert_eq!(11, byte_count);

        let expected_bytes = [0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0];
        assert_eq!(expected_bytes, buffer);
    }
}
