use std::io;

use byteorder::{ByteOrder, BigEndian};

use event::{FloEvent, EventData, OwnedFloEvent, FloEventId, Timestamp, time};
use engine::event_stream::partition::segment::mmap::{MmapRef};



#[derive(Debug, Clone)]
pub struct PersistentEvent {
    id: FloEventId,
    file_offset: usize,
    raw_data: MmapRef,
}


impl PersistentEvent {

    pub fn get_repr_length<E: FloEvent>(event: &E) -> u32 {
        // Don't change this function without also changing `write_event` below!
        //
        // 4 for total_size +     start = 0
        // 8 for header marker +  start = 8
        // 4 for crc +            start = 12
        // 10 for id +            start = 16
        // 10 for parent_id +     start = 26
        // 8 for timestamp +      start = 36
        // 4 for namespace.len +  start = 44
        // x for namespace +      start = 48
        // 4 for data.len +       start = 48 + x = ?
        // y for data             start = 52 + x = ?
        //
        // = 52 + x + y
        52u32 + event.namespace().len() as u32 + event.data_len()
    }

    pub fn total_repr_len(&self) -> usize {
        PersistentEvent::get_repr_length(self) as usize
    }

    pub unsafe fn write_unchecked<E: FloEvent>(event: &E, buffer: &mut [u8]) {
        let len = PersistentEvent::get_repr_length(event);
        write_event_unchecked(buffer, event, len);
    }

    pub fn read(mmap: &MmapRef, start_offset: usize) -> io::Result<Self> {
        let result = {
            let buffer = mmap.get_read_slice(start_offset);
            PersistentEvent::validate(buffer)
        };
        result.and_then(|(id, total_size)| {
            PersistentEvent::from_raw( id, mmap.clone(), start_offset, total_size as usize)
        })
    }

    pub fn file_offset(&self) -> usize {
        self.file_offset
    }

    // TODO: remove data_len field from PersistentEvent
    fn from_raw(id: FloEventId, mmap: MmapRef, start_offset: usize, _data_len: usize) -> io::Result<PersistentEvent> {
        Ok(PersistentEvent {
            id: id,
            file_offset: start_offset,
            raw_data: mmap,
        })
    }

    fn validate(buffer: &[u8]) -> io::Result<(FloEventId, u32)> {
        if buffer.len() < 52 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "buffer is not large enough"));
        }

        let total_len = BigEndian::read_u32(&buffer[..4]);

        let header_bytes = &buffer[4..12];
        if header_bytes != b"FLO_EVT\n" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid marker bytes"));
        }

        let crc_buf = &buffer[12..16];
        let crc = BigEndian::read_u32(crc_buf);

        let partition_buf = &buffer[16..18];
        let partition_num = BigEndian::read_u16(partition_buf);
        let counter_buf = &buffer[18..26];
        let counter = BigEndian::read_u64(counter_buf);

        // check the namespace and data lengths to ensure that they line up OK
        let ns_len = BigEndian::read_u32(&buffer[44..48]);

        if ns_len as usize + 52 > buffer.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "namespace length too large"));
        }

        let data_len_pos = 48usize + ns_len as usize;
        let data_len_buf = &buffer[data_len_pos..(data_len_pos + 4)];
        let data_len = BigEndian::read_u32(data_len_buf);

        if total_len != 52 + ns_len + data_len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "mismatched lengths"));
        }

        Ok((FloEventId::new(partition_num, counter), total_len))
    }

    fn as_buf(&self, start: usize, len: usize) -> &[u8] {
        &self.raw_data.get_read_slice(self.file_offset + start)[..len]
    }

    fn namespace_len(&self) -> u32 {
        let buf = self.as_buf(44, 4);
        BigEndian::read_u32(buf)
    }
}

impl PartialEq for PersistentEvent {
    fn eq(&self, other: &PersistentEvent) -> bool {
        self.id() == other.id() &&
                self.parent_id() == other.parent_id() &&
                self.namespace() == other.namespace() &&
                self.timestamp() == other.timestamp() &&
                self.data() == other.data()
    }
}

impl EventData for PersistentEvent {
    fn event_namespace(&self) -> &str {
        let ns_len = self.namespace_len() as usize;
        let ns_buf = self.as_buf(48, ns_len);
        unsafe {
            ::std::str::from_utf8_unchecked(ns_buf)
        }
    }

    fn event_parent_id(&self) -> Option<FloEventId> {
        let buf = self.as_buf(26, 10);
        let partition = BigEndian::read_u16(&buf[0..2]);
        let counter = BigEndian::read_u64(&buf[2..]);
        if counter > 0 {
            Some(FloEventId::new(partition, counter))
        } else {
            None
        }
    }

    fn event_data(&self) -> &[u8] {
        self.data()
    }

    fn get_precomputed_crc(&self) -> Option<u32> {
        let buf = self.as_buf(8, 4);
        Some(BigEndian::read_u32(buf))
    }
}

impl FloEvent for PersistentEvent {
    fn id(&self) -> &FloEventId {
        &self.id
    }

    fn timestamp(&self) -> Timestamp {
        let buf = self.as_buf(36, 8);
        let as_u64 = BigEndian::read_u64(buf);
        time::from_millis_since_epoch(as_u64)
    }

    fn parent_id(&self) -> Option<FloEventId> {
        self.event_parent_id()
    }

    fn namespace(&self) -> &str {
        self.event_namespace()
    }

    fn data_len(&self) -> u32 {
        let ns_len = self.namespace_len() as usize;
        let data_len_buf = self.as_buf(48 + ns_len, 4);
        BigEndian::read_u32(data_len_buf)
    }

    fn data(&self) -> &[u8] {
        let ns_len = self.namespace_len() as usize;
        let data_len = self.data_len() as usize;
        self.as_buf(52 + ns_len, data_len)
    }

    fn to_owned_event(&self) -> OwnedFloEvent {
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
    // 8 for header marker +  start = 8
    // 4 for crc +            start = 12
    // 10 for id +            start = 16
    // 10 for parent_id +     start = 26
    // 8 for timestamp +      start = 36
    // 4 for namespace.len +  start = 44
    // x for namespace +      start = 48
    // 4 for data.len +       start = 48 + x = ?
    // y for data             start = 52 + x = ?
    //
    // = 52 + x + y

    Serializer::new(buffer)
            .write_u32(total_size)
            .write_bytes(b"FLO_EVT\n")
            .write_u32(event.get_or_compute_crc())
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


