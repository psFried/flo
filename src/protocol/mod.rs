mod client;
mod server;

use std::io::{self, Read, Write};
use std::cmp;

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
                // Interrupted is different from WouldBlock. Even with non-blocking I/O, I think we still want to ignore these
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
        trace!("Returning buffer: {:?}", buf);
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

pub struct MessageReader<T> {
    io: T,
    buffer: Buffer,
    current_message: Option<InProgressMessage>,
}

impl <T> MessageReader<T> where T: Read {

    pub fn read_next(&mut self) -> io::Result<ProtocolMessage> {
        use nom::IResult;

        let MessageReader{ref mut io, ref mut buffer, ref mut current_message} = *self;
        // if there's an in-progress message, then try to push the bytes into it
        // otherwise try to deserialize a new message

        let (bytes_consumed, mut next_message) = current_message.take().map(|in_progress_message| {
            Ok((0, in_progress_message))
        }).unwrap_or_else(|| {
            buffer.fill(io).and_then(|bytes| {
                let buffer_start_length = bytes.len();
                match self::client::parse_any(bytes) {
                    IResult::Done(remaining, message) => {
                        let bytes_used = buffer_start_length - remaining.len();
                        Ok((bytes_used, InProgressMessage::new(message)))
                    }
                    IResult::Error(err) => {
                        Err(io::Error::new(io::ErrorKind::InvalidData, format!("Error parsing message: {:?}", err)))
                    }
                    IResult::Incomplete(need) => {
                        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Not enough data to deserialize message"))
                    }
                }
            })
        })?; // Early return if either the read or parse fails

        buffer.consume(bytes_consumed);

        let mut remaining_bytes_in_body = next_message.body_bytes_remaining();

        while remaining_bytes_in_body > 0 {
            let bytes = buffer.fill(io)?; // early return if the read fails
            remaining_bytes_in_body = next_message.append_body(bytes);
        }
        Ok(next_message.finish())
    }
}


struct InProgressMessage {
    message: ProtocolMessage,
    body_read_pos: usize,
}

impl InProgressMessage {
    fn new(message: ProtocolMessage) -> InProgressMessage {
        InProgressMessage {
            message: message,
            body_read_pos: 0,
        }
    }

    fn body_bytes_remaining(&mut self) -> usize {
        let pos = self.body_read_pos;
        get_body_buffer(&mut self.message).map(|buf| buf.capacity() - pos).unwrap_or(0)
    }

    fn append_body(&mut self, bytes: &[u8]) -> usize {
        let InProgressMessage {ref mut message, ref mut body_read_pos} = *self;

        let mut total_body_size = 0;
        let n_appended = get_body_buffer(message).map(|message_buffer| {
            total_body_size = message_buffer.capacity();
            copy_until_capacity(bytes, message_buffer)
        }).unwrap_or(0);

        *body_read_pos += n_appended;
        total_body_size - *body_read_pos
    }

    fn finish(self) -> ProtocolMessage {
        self.message
    }
}

fn get_body_buffer(message: &mut ProtocolMessage) -> Option<&mut Vec<u8>> {
    match *message {
        ProtocolMessage::NewProduceEvent(ref mut event) => Some(&mut event.data),
        _ => None
    }
}

fn copy_until_capacity(src: &[u8], dst: &mut Vec<u8>) -> usize {
    let len = cmp::min(src.len(), dst.capacity() - dst.len());
    let s = &src[..len];
    dst.extend_from_slice(s);
    len
}



