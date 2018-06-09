
use std::fmt::{self, Display};
use serializer::{Serializer, FloSerialize};

pub type FloInstanceId = u64;

pub const NULL_INSTANCE_ID: FloInstanceId = 0;

pub fn generate_new() -> FloInstanceId {
    let mut value = 0;
    while value == 0 {
        value = ::rand::random();
    }
    value
}

pub fn as_bytes(instance_id: FloInstanceId) -> [u8; 8] {
    use ::byteorder::{ByteOrder, BigEndian};

    let mut bytes = [0; 8];
    BigEndian::write_u64(&mut bytes[..], instance_id);
    bytes
}

pub fn from_bytes(bytes: &[u8]) -> FloInstanceId {
    use ::byteorder::{ByteOrder, BigEndian};

    let b = &bytes[0..8];
    BigEndian::read_u64(b)
}


named!{pub parse_flo_instance_id<FloInstanceId>, map_res!(::nom::be_u64, |val| {
    if val == 0 {
        Err(())
    } else {
        Ok(val)
    }
})}

named!{pub parse_optional_flo_instance_id<Option<FloInstanceId>>, map!(::nom::be_u64, |val| {
    if val > 0 {
        Some(val)
    } else {
        None
    }
} )}
