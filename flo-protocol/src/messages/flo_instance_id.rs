
use std::fmt::{self, Display};
use serializer::{Serializer, FloSerialize};

/// An opaque identifier used to uniquely identify flo instances.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct FloInstanceId(u64);

impl FloInstanceId {

    pub fn generate_new() -> FloInstanceId {
        FloInstanceId(::rand::random())
    }

    pub fn as_bytes(&self) -> [u8; 8] {
        use ::byteorder::{ByteOrder, BigEndian};

        let mut bytes = [0; 8];
        BigEndian::write_u64(&mut bytes[..], self.0);
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> FloInstanceId {
        use ::byteorder::{ByteOrder, BigEndian};

        let b = &bytes[0..8];
        let num = BigEndian::read_u64(b);
        FloInstanceId(num)
    }
}

impl Display for FloInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FloInstanceId({})", self.0)
    }
}


impl FloSerialize for FloInstanceId {
    fn serialize<'a>(&'a self, serializer: Serializer<'a>) -> Serializer<'a> {
        serializer.write_u64(self.0)
    }
}

named!{pub parse_flo_instance_id<FloInstanceId>, map!(::nom::be_u64, |val| {FloInstanceId(val) } )}
