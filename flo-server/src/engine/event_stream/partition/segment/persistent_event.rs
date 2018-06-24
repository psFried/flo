use std::io;

use byteorder::{ByteOrder, BigEndian};

use event::{FloEvent, EventData, OwnedFloEvent, FloEventId, Timestamp, time};
use engine::event_stream::partition::segment::mmap::{MmapRef};


mod offsets {
    pub const TOTAL_LEN_START: usize = 0;
    pub const TOTAL_LEN_LEN: usize = 4;

    pub const HEADER_MARKER_START: usize = TOTAL_LEN_START + TOTAL_LEN_LEN;
    pub const HEADER_MARKER_LEN: usize = 8;

    pub const CRC_START: usize = HEADER_MARKER_START + HEADER_MARKER_LEN;
    pub const CRC_LEN: usize = 4;

    pub const ID_START: usize = CRC_START + CRC_LEN;

    pub const ID_PARTITION_LEN: usize = 2;
    pub const ID_COUNTER_LEN: usize = 8;
    pub const ID_LEN: usize = ID_PARTITION_LEN + ID_COUNTER_LEN;

    pub const PARENT_ID_START: usize = ID_START + ID_LEN;

    pub const TIMESTAMP_START: usize = PARENT_ID_START + ID_LEN;
    pub const TIMESTAMP_LEN: usize = 8;

    pub const NS_LEN_START: usize = TIMESTAMP_START + TIMESTAMP_LEN;
    pub const NS_LEN_LEN: usize = 4;

    pub const NS_BYTES_START: usize = NS_LEN_START + NS_LEN_LEN;

    pub fn data_len_start(namespace_len: usize) -> usize {
        NS_BYTES_START + namespace_len
    }
    pub const DATA_LEN_LEN: usize = 4;

    pub const MIN_EVENT_LEN: usize = NS_BYTES_START + DATA_LEN_LEN;
}

static HEADER_NORMAL: &[u8; 8] = b"FLO_EVT\n";
static HEADER_DELETED: &[u8; 8] = b"FLO_DEL\n";

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
        // 8 for header marker +  start = 4
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
        offsets::MIN_EVENT_LEN as u32 + event.namespace().len() as u32 + event.data_len()
    }

    pub fn total_repr_len(&self) -> usize {
        PersistentEvent::get_repr_length(self) as usize
    }

    pub fn is_deleted(&self) -> bool {
        let marker_buf = self.as_buf(offsets::HEADER_MARKER_START, offsets::HEADER_MARKER_LEN);
        marker_buf != &HEADER_NORMAL[..]
    }

    pub unsafe fn set_deleted(&mut self) {
        use std::io::Write;

        let start_offset = self.file_offset + offsets::HEADER_MARKER_START;
        let mut buffer = self.raw_data.get_write_slice(start_offset);
        // this write can't really fail, since we already know that there's space in the slice
        let _ = buffer.write_all(HEADER_DELETED);
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

    //TODO: currently PersistentEvent does not validate the crc. Not sure if it's worth it or not
    fn validate(buffer: &[u8]) -> io::Result<(FloEventId, u32)> {
        use self::offsets::*;

        if buffer.len() < MIN_EVENT_LEN {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "buffer is not large enough"));
        }

        let total_len = BigEndian::read_u32(&buffer[..TOTAL_LEN_LEN]);

        let header_bytes = &buffer[HEADER_MARKER_START..(HEADER_MARKER_START + HEADER_MARKER_LEN)];
        if header_bytes != HEADER_NORMAL && header_bytes != HEADER_DELETED {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid marker bytes"));
        }

        let partition_buf = &buffer[ID_START..(ID_START + ID_PARTITION_LEN)];
        let partition_num = BigEndian::read_u16(partition_buf);
        let counter_buf = &buffer[(ID_START + ID_PARTITION_LEN)..(ID_START + ID_LEN)];
        let counter = BigEndian::read_u64(counter_buf);

        // check the namespace and data lengths to ensure that they line up OK
        let ns_len = BigEndian::read_u32(&buffer[NS_LEN_START..(NS_LEN_START + NS_LEN_LEN)]);

        if ns_len as usize + MIN_EVENT_LEN > buffer.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "namespace length too large"));
        }

        let data_len_pos = data_len_start(ns_len as usize);
        let data_len_buf = &buffer[data_len_pos..(data_len_pos + DATA_LEN_LEN)];
        let data_len = BigEndian::read_u32(data_len_buf);

        if total_len != MIN_EVENT_LEN as u32 + ns_len + data_len {
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
            .write_bytes(HEADER_NORMAL)
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


