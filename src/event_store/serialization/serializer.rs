use std::io::{self, Read, Cursor};
use event::{EventId, Event};

use byteorder::{ByteOrder, BigEndian, LittleEndian};

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

pub fn size_on_disk(event: &Event) -> usize {
    // 8 bytes for event id
    // 4 bytes for data length
    event.data.len() + 8 + 4
}

const U64_AS_BYTES_LENGTH: usize = 8;

impl <'a> Read for EventSerializer<'a> {
    // TODO: Read should be able to leave off in the middle of writing the event id or data length
    fn read(&mut self, mut buffer: &mut [u8]) -> io::Result<usize> {
        if self.cursor.position() == 0 {
            BigEndian::write_u64(buffer, self.event_id);
            let mut buffer = &mut buffer[U64_AS_BYTES_LENGTH ..];
            BigEndian::write_u32(buffer, self.cursor.get_ref().len() as u32);

            let mut buffer = &mut buffer[4 ..];
            let byte_count = 12;
            let free_space = buffer.len();

            self.cursor.read(&mut buffer).map(|count| count + byte_count)
        } else {
            Ok(0)
        }
    }
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

        let mut buffer = [0; 16];

        let result = serializer.read(&mut buffer);

        let byte_count = result.unwrap();
        assert_eq!(15, byte_count);

        let expected_bytes = [
            0, 0, 0, 0, 0, 0, 0, 1, //event id
            0, 0, 0, 3,             //length of data section
            1, 2, 3,                //data section
            0                       //unused portion of buffer
        ];
        assert_eq!(expected_bytes, buffer);
    }

    #[test]
    fn returns_0_if_event_was_already_read() {
        let event = Event::new(1, vec![1, 2, 3]);
        let mut serializer = EventSerializer::new(&event);
        let mut buffer = [0; 40];
        serializer.read(&mut buffer).unwrap();
        let result2 = serializer.read(&mut buffer).unwrap();
        assert_eq!(0, result2);
    }

    #[test]
    fn size_on_disk_returns_same_size_as_event_takes_when_written() {
        let event = Event::new(1, vec![1, 2, 3]);
        let size_on_disk = size_on_disk(&event);

        let mut serializer = EventSerializer::new(&event);
        let mut buffer = [0; 40];
        let write_size = serializer.read(&mut buffer).unwrap();
        assert_eq!(write_size, size_on_disk);
    }
}
