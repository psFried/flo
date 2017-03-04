mod client;
mod server;

use std::io::{self, Read, Write};

pub use self::client::*;
pub use self::server::{ServerMessage, ServerProtocol, ServerProtocolImpl};

pub const BUFFER_LENGTH: usize = 8 * 1024;

pub struct Buffer {
    bytes: Vec<u8>,
    pos: usize,
    len: usize,
}

fn read<R: Read>(buffer: &mut [u8], reader: &mut R) -> io::Result<usize> {
    loop {
        match reader.read(buffer) {
            Ok(n) => {
                return Ok(n);
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                trace!("Interrupted reading from socket, trying again");
            },
            Err(io_err) => {
                return Err(io_err);
            }
        }
    }
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            bytes: vec![0; BUFFER_LENGTH],
            pos: 0,
            len: 0
        }
    }

    pub fn fill<R: Read>(&mut self, reader: &mut R) -> io::Result<&[u8]> {
        if self.pos >= self.len {
            let nread = {
                let mut buf = &mut self.bytes[..];
                read(buf, reader)?
            };
            trace!("read {} bytes", nread);
            self.len = nread;
            self.pos = 0;
        }
        let buf = &self.bytes[self.pos..self.len];
        debug!("Returning buffer: {:?}", buf);
        Ok(buf)
    }

    pub fn drain(&mut self, num_bytes: usize) -> &[u8] {
        let pos = self.pos;
        let byte_count = ::std::cmp::min(num_bytes, self.len - pos);
        self.consume(byte_count);
        &self.bytes[pos..(pos + byte_count)]
    }

    pub fn consume(&mut self, nbytes: usize) {
        self.pos += nbytes;
    }
}

impl ::std::ops::Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.bytes[self.pos..self.len]
    }
}


pub struct MessageWriter<T> {
    io: T,
    buffer: [u8; BUFFER_LENGTH],
    current_message: Option<ProtocolMessage>,
    data_position: usize,
}

