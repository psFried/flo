use std::io;

use memmap::MmapViewSync;
use byteorder::{ByteOrder, BigEndian};

use event::{FloEvent, OwnedFloEvent, FloEventId, Timestamp, time};



#[derive(Debug)]
pub struct PersistentEvent {
    id: FloEventId,
    file_offset: usize,
    raw_data: MmapViewSync,
}


impl PersistentEvent {

    pub fn get_repr_length<E: FloEvent>(event: &E) -> u32 {
        // Don't change this function without also changing `write_event` below!
        //
        // 4 for total_size +     start = 0
        // 8 for header marker +  start = 4
        // 10 for id +            start = 12
        // 10 for parent_id +     start = 22
        // 8 for timestamp +      start = 32
        // 4 for namespace.len +  start = 40
        // x for namespace +      start = 44
        // 4 for data.len +       start = 44 + x = ?
        // y for data             start = 48 + x = ?
        //
        // = 48 + x + y
        48u32 + event.namespace().len() as u32 + event.data_len()
    }

    pub fn total_repr_len(&self) -> usize {
        PersistentEvent::get_repr_length(self) as usize
    }

    pub unsafe fn write<E: FloEvent>(event: &E,
                                     start_offset: usize,
                                     mmap: &mut MmapViewSync) -> io::Result<Self> {
        let len = PersistentEvent::get_repr_length(event);

        {
            let mmap_buf = mmap.as_mut_slice();
            write_event_unchecked(&mut mmap_buf[start_offset..], event, len);
        }

        PersistentEvent::from_raw(*event.id(), mmap, start_offset, len as usize)
    }

    pub fn read(mmap: &MmapViewSync, start_offset: usize) -> io::Result<Self> {
        let buffer = unsafe {
            mmap.as_slice()
        };
        let range = &buffer[start_offset..];
        PersistentEvent::validate(range).and_then(|(id, total_size)| {
            PersistentEvent::from_raw( id, mmap, start_offset, total_size as usize)
        })
    }

    pub fn file_offset(&self) -> usize {
        self.file_offset
    }

    fn from_raw(id: FloEventId, mmap: &MmapViewSync, start_offset: usize, data_len: usize) -> io::Result<PersistentEvent> {
        let mut view = unsafe {
            mmap.clone()
        };
        view.restrict(start_offset, data_len)?;

        Ok(PersistentEvent {
            id: id,
            file_offset: start_offset,
            raw_data: view,
        })
    }

    fn validate(buffer: &[u8]) -> io::Result<(FloEventId, u32)> {
        if buffer.len() < 48 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "buffer is not large enough"));
        }

        let total_len = BigEndian::read_u32(&buffer[..4]);

        let header_bytes = &buffer[4..12];
        if header_bytes != b"FLO_EVT\n" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid marker bytes"));
        }

        let partition_buf = &buffer[12..14];
        let partition_num = BigEndian::read_u16(partition_buf);
        let counter_buf = &buffer[14..22];
        let counter = BigEndian::read_u64(counter_buf);

        // check the namespace and data lengths to ensure that they line up OK
        let ns_len = BigEndian::read_u32(&buffer[40..44]);

        if ns_len as usize + 48 > buffer.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "namespace length too large"));
        }

        let data_len_pos = 44usize + ns_len as usize;
        let data_len_buf = &buffer[data_len_pos..(data_len_pos + 4)];
        let data_len = BigEndian::read_u32(data_len_buf);

        if total_len != 48 + ns_len + data_len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "mismatched lengths"));
        }

        Ok((FloEventId::new(partition_num, counter), total_len))
    }

    fn as_buf(&self, start: usize, len: usize) -> &[u8] {
        unsafe {
            &self.raw_data.as_slice()[start..(start + len)]
        }
    }

    fn namespace_len(&self) -> u32 {
        let buf = self.as_buf(40, 4);
        BigEndian::read_u32(buf)
    }
}

impl FloEvent for PersistentEvent {
    fn id(&self) -> &FloEventId {
        &self.id
    }

    fn timestamp(&self) -> Timestamp {
        let buf = self.as_buf(32, 8);
        let as_u64 = BigEndian::read_u64(buf);
        time::from_millis_since_epoch(as_u64)
    }

    fn parent_id(&self) -> Option<FloEventId> {
        let buf = self.as_buf(22, 10);
        let partition = BigEndian::read_u16(&buf[0..2]);
        let counter = BigEndian::read_u64(&buf[2..]);
        if counter > 0 {
            Some(FloEventId::new(partition, counter))
        } else {
            None
        }
    }

    fn namespace(&self) -> &str {
        let ns_len = self.namespace_len() as usize;
        let ns_buf = self.as_buf(44, ns_len);
        unsafe {
            ::std::str::from_utf8_unchecked(ns_buf)
        }
    }

    fn data_len(&self) -> u32 {
        let ns_len = self.namespace_len() as usize;
        let data_len_buf = self.as_buf(44 + ns_len, 4);
        BigEndian::read_u32(data_len_buf)
    }

    fn data(&self) -> &[u8] {
        let ns_len = self.namespace_len() as usize;
        let data_len = self.data_len() as usize;
        self.as_buf(48 + ns_len, data_len)
    }

    fn to_owned(&self) -> OwnedFloEvent {
        let id = *self.id();
        let parent_id = self.parent_id();
        let timestamp = self.timestamp();
        let namespace = self.namespace().to_owned();
        let data = self.data().to_owned();
        OwnedFloEvent::new(id, parent_id, timestamp, namespace, data)
    }
}


/// private function to write the event. `total_size` must match the actual size of the data to be written
fn write_event_unchecked<E: FloEvent>(buffer: &mut [u8], event: &E, total_size: u32) {
    use event::time::millis_since_epoch;
    use protocol::serializer::Serializer;

    // Don't change this function without also changing `get_repr_len` above!
    //
    // 4 for total_size +     start = 0
    // 8 for header marker +  start = 4
    // 10 for id +            start = 12
    // 10 for parent_id +     start = 22
    // 8 for timestamp +      start = 32
    // 4 for namespace.len +  start = 40
    // x for namespace +      start = 44
    // 4 for data.len +       start = 44 + x = ?
    // y for data             start = 48 + x = ?
    //
    // = 48 + x + y

    Serializer::new(buffer)
            .write_u32(total_size)
            .write_bytes(b"FLO_EVT\n")
            .write_u16(event.id().actor)
            .write_u64(event.id().event_counter)
            .write_u16(event.parent_id().map(|e| e.actor).unwrap_or(0))
            .write_u64(event.parent_id().map(|e| e.event_counter).unwrap_or(0))
            .write_u64(millis_since_epoch(event.timestamp()))
            .write_u32(event.namespace().len() as u32)
            .write_bytes(event.namespace().as_bytes())
            .write_u32(event.data_len())
            .write_bytes(event.data())
            .finish();
}

#[cfg(test)]
mod test {
    use super::*;
    use memmap::{Mmap, Protection};
    use event::{OwnedFloEvent, FloEvent, FloEventId, time};

    #[test]
    fn write_and_read_an_event_with_zero_lengh_namespace_and_data() {
        let mut mmap = Mmap::anonymous(1024, Protection::ReadWrite).unwrap().into_view_sync();
        let input = OwnedFloEvent::new(
            FloEventId::new(3, 4),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "".to_owned(),
            Vec::new());

        unsafe {
            let result = PersistentEvent::write(&input, 0, &mut mmap);
            let persisted = result.expect("failed to write event");
            assert_events_eq(&input, &persisted);
        }
    }

    #[test]
    fn read_event_returns_error_when_namespace_length_is_too_large() {
        assert_read_err("namespace length too large", |buf| {
            buf[41] = 56;
        })
    }

    #[test]
    fn read_event_returns_error_when_namespace_length_is_too_small() {
        assert_read_err("mismatched lengths", |buf| {
            buf[43] = 7; //make the namespace length 7 instead of 8
        })
    }

    #[test]
    fn read_event_returns_error_when_data_length_is_too_large() {
        assert_read_err("mismatched lengths", |buf| {
            buf[55] = 6; //make the data length 6 instead of 5
        })
    }

    #[test]
    fn read_event_returns_error_when_data_length_is_too_small() {
        assert_read_err("mismatched lengths", |buf| {
            buf[55] = 4; //make the data length 4 instead of 5
        })
    }


    #[test]
    fn write_an_event_and_read_it_back() {
        use memmap::{Mmap, Protection};

        let mut mmap = Mmap::anonymous(1024, Protection::ReadWrite).unwrap().into_view_sync();

        let input = OwnedFloEvent::new(
            FloEventId::new(3, 4),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "/foo/bar".to_owned(),
            vec![1, 2, 3, 4, 5]);

        unsafe {
            let result = PersistentEvent::write( &input, 0, &mut mmap);
            let persisted = result.expect("failed to write event");
            assert_events_eq(&input, &persisted);
        }

        let len = PersistentEvent::get_repr_length(&input);
        let result = PersistentEvent::read(&mmap, 0);
        let persisted = result.expect("failed to read event");
        assert_events_eq(&input, &persisted);
        assert_eq!(len, PersistentEvent::get_repr_length(&persisted));
    }

    fn assert_events_eq<L: FloEvent, R: FloEvent>(lhs: &L, rhs: &R) {
        assert_eq!(lhs.id(), rhs.id());
        assert_eq!(lhs.parent_id(), rhs.parent_id());
        assert_eq!(lhs.timestamp(), rhs.timestamp());
        assert_eq!(lhs.namespace(), rhs.namespace());
        assert_eq!(lhs.data_len(), rhs.data_len());
        assert_eq!(lhs.data(), rhs.data());
    }

    fn assert_read_err<F: Fn(&mut [u8])>(expected_description: &str, modify_buffer_fun: F) {
        use std::error::Error;
        use memmap::{Mmap, Protection};

        let mut mmap = Mmap::anonymous(1024, Protection::ReadWrite).unwrap().into_view_sync();
        let input = OwnedFloEvent::new(
            FloEventId::new(3, 4),
            Some(FloEventId::new(3, 3)),
            time::from_millis_since_epoch(999),
            "/foo/bar".to_owned(),
            vec![1, 2, 3, 4, 5]);
        let input_len = PersistentEvent::get_repr_length(&input);

        unsafe {
            let result = PersistentEvent::write(&input, 0, &mut mmap);
            let event = result.expect("failed to write event");
            let result_len = PersistentEvent::get_repr_length(&event);
            assert_eq!(input_len, result_len);

            // modify the buffer
            modify_buffer_fun(mmap.as_mut_slice());
        }


        let err_result = PersistentEvent::read(&mmap, 0);
        assert!(err_result.is_err());
        let io_err = err_result.unwrap_err();
        assert_eq!(expected_description, io_err.description());
    }
}
