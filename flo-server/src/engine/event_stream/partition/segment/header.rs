
use std::io;

use byteorder::{ByteOrder, BigEndian};
use memmap::Mmap;

use event::{Timestamp, time};

#[derive(Debug)]
pub struct SegmentHeader {
    pub create_time: Timestamp,
    pub end_time: Timestamp,
}


impl SegmentHeader {

    pub fn read(mmap: &Mmap) -> io::Result<SegmentHeader> {
        let data = unsafe { mmap.as_slice() };

        if data.len() < SegmentHeader::get_repr_length() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Segment file length is smaller than header length"));
        }

        let create_ts_milis = BigEndian::read_u64(&data[0..8]);
        let end_ts_millis = BigEndian::read_u64(&data[8..16]);
        trace!("reading header from start: {}, end: {}", create_ts_milis, end_ts_millis);

        let create = time::from_millis_since_epoch(create_ts_milis);
        let end = time::from_millis_since_epoch(end_ts_millis);

        Ok(SegmentHeader{
            create_time: create,
            end_time: end
        })
    }

    pub fn write(&self, mmap: &mut Mmap) -> io::Result<()> {

        let dst = unsafe { mmap.as_mut_slice() };

        if dst.len() < SegmentHeader::get_repr_length() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Destination length is smaller than header length"));
        }

        let create_ts_millis = time::millis_since_epoch(self.create_time);
        BigEndian::write_u64(&mut dst[0..8], create_ts_millis);

        let end_ts_millis = time::millis_since_epoch(self.end_time);
        BigEndian::write_u64(&mut dst[8..16], end_ts_millis);

        trace!("wrote header {:?} as start: {}, end: {}", self, create_ts_millis, end_ts_millis);
        Ok(())
    }

    pub fn get_repr_length() -> usize {
        16
    }
}

