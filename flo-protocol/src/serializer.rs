use std::net::SocketAddr;
use byteorder::{ByteOrder, BigEndian};

pub struct Serializer<'a> {
    buffer: &'a mut [u8],
    position: usize,
}


impl <'a> Serializer<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Serializer<'a> {
        Serializer {
            buffer: buffer,
            position: 0,
        }
    }

    pub fn write_bool(self, value: bool) -> Self {
        let b = if value { 1 } else { 0 };
        self.write_u8(b)
    }

    pub fn write_u8(mut self, byte: u8) -> Self {
        let pos = self.position;
        self.buffer[pos] = byte;
        self.position += 1;
        self
    }

    pub fn write_u16(mut self, number: u16) -> Self {
        let pos = self.position;
        BigEndian::write_u16(&mut self.buffer[pos..(pos+2)], number);
        self.position += 2;
        self
    }

    pub fn write_u32(mut self, n: u32) -> Self {
        let pos = self.position;
        BigEndian::write_u32(&mut self.buffer[pos..(pos+4)], n);
        self.position += 4;
        self
    }

    pub fn write_u64(mut self, n: u64) -> Self {
        let pos = self.position;
        BigEndian::write_u64(&mut self.buffer[pos..(pos+8)], n);
        self.position += 8;
        self
    }

    pub fn write_bytes<T: AsRef<[u8]>>(mut self, bytes: T) -> Self {
        let pos = self.position;
        let write_len = bytes.as_ref().len();
        self.buffer[pos..(pos+write_len)].copy_from_slice(bytes.as_ref());
        self.position += write_len;
        self
    }

    /// Writes a string prepended by a u16 length
    pub fn write_string<S: AsRef<str>>(self, string: S) -> Self {
        let string = string.as_ref();
        self.write_u16(string.len() as u16).write_bytes(string.as_bytes())
    }

    pub fn write_many<I, F, T>(self, iter: I, fun: F) -> Self
        where I: Iterator<Item=T>,
              F: Fn(Self, T) -> Self {

        let mut serializer = self;

        for item in iter {
            serializer = fun(serializer, item);
        }
        serializer
    }

    pub fn write_socket_addr(self, addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => {
                self.write_u8(4u8)
                        .write_bytes(v4.ip().octets())
                        .write_u16(v4.port())
            }
            SocketAddr::V6(v6) => {
                // TODO: would it be correct to also serialize flowinfo and scope_id for ipv6 addresses?
                self.write_u8(6)
                        .write_bytes(v6.ip().octets())
                        .write_u16(v6.port())
            }
        }
    }

    pub fn finish(self) -> usize {
        self.position
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};

    #[test]
    fn u8_is_written() {
        let mut buffer = [0; 64];
        let value: u8 = 9;
        {
            let mut subject = Serializer::new(&mut buffer[..]);
            subject = subject.write_u8(value);
            assert_eq!(1, subject.position);
        }
        let result = buffer[0];
        assert_eq!(value, result);
    }

    #[test]
    fn u16_is_written() {
        let mut buffer = [0; 64];
        let value = 7777;
        {
            let mut subject = Serializer::new(&mut buffer[..]);
            subject = subject.write_u16(value);
            assert_eq!(2, subject.position);
        }
        let result = BigEndian::read_u16(&buffer[..2]);
        assert_eq!(value, result);
    }

    #[test]
    fn u32_is_written() {
        let mut buffer = [0; 64];
        let value = 75636455;
        {
            let mut subject = Serializer::new(&mut buffer[..]);
            subject = subject.write_u32(value);
            assert_eq!(4, subject.position);
        }
        let result = BigEndian::read_u32(&buffer[..4]);
        assert_eq!(value, result);
    }

    #[test]
    fn u64_is_written() {
        let mut buffer = [0; 64];
        let value = 9875636455;
        {
            let mut subject = Serializer::new(&mut buffer[..]);
            subject = subject.write_u64(value);
            assert_eq!(8, subject.position);
        }
        let result = BigEndian::read_u64(&buffer[..8]);
        assert_eq!(value, result);
    }

    #[test]
    fn raw_bytes_are_written() {
        let mut buffer = [0; 64];
        let value = b"bacon";
        {
            let mut subject = Serializer::new(&mut buffer[..]);
            subject = subject.write_bytes(value);
            assert_eq!(value.len(), subject.position);
        }
        assert_eq!(&value[..], &buffer[..5]);
    }

    #[test]
    fn string_is_written() {
        let mut buffer = [0; 64];
        let value = "bacon and eggs";
        {
            let mut subject = Serializer::new(&mut buffer[..]);
            subject = subject.write_string(value);
            assert_eq!(value.len() + 2, subject.position);
        }
        let mut expected = vec![0, 14];
        expected.extend_from_slice(value.as_bytes());
        assert_eq!(&expected[..], &buffer[..(value.len()+2)]);
    }

    #[test]
    fn multiple_values_are_written_in_sequence() {
        let mut buffer = [0; 64];
        let subject = Serializer::new(&mut buffer[..]);
        let result = subject.write_u16(987).write_u64(23).write_string("bacon").finish();
        assert_eq!(17, result);
    }

    fn address(addr: &str) -> SocketAddr {
        ::std::str::FromStr::from_str(addr).unwrap()
    }

    #[test]
    fn ipv4_socket_address_is_written() {
        let mut buffer = [0; 64];
        let result = {
            let subject = Serializer::new(&mut buffer[..]);
            subject.write_socket_addr(address("123.45.67.8:80")).finish()
        };
        let expected = [4, 123, 45, 67, 8, 0, 80];
        assert_eq!(&expected, &buffer[..result]);
    }

    #[test]
    fn ipv6_address_is_written() {
        let mut buffer = [0; 64];
        let result = {
            let subject = Serializer::new(&mut buffer[..]);
            let addr = address("[2001:db8::1]:8080");
            subject.write_socket_addr(addr).finish()
        };
        let expected = [6, 32,1, 13,184, 0,0, 0,0, 0,0, 0,0, 0,0, 0,1, 31,144];
        assert_eq!(&expected, &buffer[..result]);
    }
}
