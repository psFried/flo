#[macro_use]
extern crate nom;

#[macro_use]
extern crate log;

extern crate flo_event as event;
extern crate byteorder;

pub mod serializer;
mod client;

use std::io::{self, Read, Write};
use std::cmp;
use std::borrow::Cow;

pub use self::client::*;

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
            Err(ref e) if cfg!(target_os = "macos") && e.raw_os_error() == Some(41) => {
                /*
                This is a specific condition that can happen on OS X
                refer to: http://erickt.github.io/blog/2014/11/19/adventures-in-debugging-a-potential-osx-kernel-bug/
                for more details
                */
                debug!(target: "eprototype", "Retrying read due to error: {:?}", e);
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
            if nread == 0 {
                warn!("Read 0 bytes, returning UnexpectedEOF");
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            trace!("read {} bytes", nread);
            self.len = nread;
            self.pos = 0;
        }
        let buf = &self.bytes[self.pos..self.len];
        trace!("Returning buffer with {} bytes", buf.len());
        Ok(buf)
    }

    pub fn grow<R: Read>(&mut self, reader: &mut R) -> io::Result<&[u8]> {
        let current_buffer_len = self.bytes.len();
        if self.len >= current_buffer_len {
            debug!("Reallocating buffer to grow the vector by {} bytes", current_buffer_len);
            self.bytes.resize(current_buffer_len * 2, 0);
        }

        let nread = {
            let mut buf = &mut self.bytes[self.len..];
            read(buf, reader)?
        };
        if nread == 0 {
            warn!("Read 0 bytes, returning UnexpectedEOF");
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        } else {
            trace!("read: {} bytes, pos: {}, old_len: {}, new_len: {}", nread, self.pos, self.len, self.len + nread);
            self.len += nread;
        }
        let buf = &self.bytes[self.pos..self.len];
        trace!("Returning buffer of {} bytes", buf.len());
        Ok(buf)
    }

    pub fn drain(&mut self, num_bytes: usize) -> &[u8] {
        let pos = self.pos;
        let byte_count = ::std::cmp::min(num_bytes, self.len - pos);
        trace!("draining buffer - requested: {}, available: {}, returning: {:?}", num_bytes, byte_count, &self.bytes[pos..(pos + byte_count)]);
        self.consume(byte_count);
        &self.bytes[pos..(pos + byte_count)]
    }

    pub fn consume(&mut self, nbytes: usize) {
        trace!("Consuming {} bytes from buffer", nbytes);
        self.pos += nbytes;
    }

}

impl ::std::ops::Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.bytes[self.pos..self.len]
    }
}

pub struct MessageStream<T> {
    io: T,
    read_buffer: Buffer,
    current_read_message: Option<InProgressMessage>,

}

impl <T> MessageStream<T> {
    pub fn new(io: T) -> MessageStream<T> {
        MessageStream {
            io: io,
            read_buffer: Buffer::new(),
            current_read_message: None,
        }
    }
}

impl <T> MessageStream<T> where T: Write {
    pub fn write(&mut self, message_writer: &mut MessageWriter) -> io::Result<()> {
        message_writer.write(&mut self.io)
    }
}

impl <T> MessageStream<T> where T: Read {

    pub fn read_next(&mut self) -> io::Result<ProtocolMessage> {
        use nom::IResult;

        let MessageStream {ref mut io, ref mut read_buffer, ref mut current_read_message} = *self;
        // if there's an in-progress message, then try to push the bytes into it
        // otherwise try to deserialize a new message

        if current_read_message.is_none() {
            let mut bytes_consumed = 0;
            let mut grow_buffer = false;
            loop {
                let bytes = if grow_buffer {
                    read_buffer.grow(io)?
                } else {
                    read_buffer.fill(io)?
                };
                let buffer_start_length = bytes.len();
                match self::client::parse_any(bytes) {
                    IResult::Done(remaining, message) => {
                        bytes_consumed += buffer_start_length - remaining.len();
                        trace!("Successful parse used {} bytes; got message: {:?}", bytes_consumed, message);
                        *current_read_message = Some(InProgressMessage::new(message));
                        break
                    }
                    IResult::Error(err) => {
                        let message = format!("Error parsing message: {:?}, buffer: {:?}",
                                              err,
                                              bytes);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, message))
                    }
                    IResult::Incomplete(_need) => {
                        grow_buffer = true;
                        trace!("Not enough data to deserialize message, trying again");
                        // loop again
                    }
                }
            }
            read_buffer.consume(bytes_consumed);
        }

        {
            let message = current_read_message.as_mut().unwrap();
            let mut remaining_bytes_in_body = message.body_bytes_remaining();

            while remaining_bytes_in_body > 0 {
                trace!("Filling body of message with {} bytes", remaining_bytes_in_body);
                let n_appended = {
                    let bytes = read_buffer.fill(io)?; // early return if the read fails
                    message.append_body(bytes)
                };
                read_buffer.consume(n_appended);
                remaining_bytes_in_body -= n_appended;
            }
        }

        Ok(current_read_message.take().unwrap().message)
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
        n_appended
    }

    fn finish(self) -> ProtocolMessage {
        self.message
    }
}

fn get_body_buffer(message: &mut ProtocolMessage) -> Option<&mut Vec<u8>> {
    match *message {
        ProtocolMessage::ProduceEvent(ref mut event) => Some(&mut event.data),
        _ => None
    }
}

fn copy_until_capacity(src: &[u8], dst: &mut Vec<u8>) -> usize {
    let len = cmp::min(src.len(), dst.capacity() - dst.len());
    let s = &src[..len];
    trace!("filling body with {} bytes", len);
    dst.extend_from_slice(s);
    len
}


pub struct MessageWriter<'a> {
    message: Cow<'a, ProtocolMessage>,
    body_position: usize,
    body_len: usize,
    header_written: bool,
}

impl <'a> MessageWriter<'a> {
    pub fn new(message: &'a ProtocolMessage) -> MessageWriter<'a> {
        MessageWriter {
            message: Cow::Borrowed(message),
            body_position: 0,
            body_len: 0,
            header_written: false,
        }
    }

    pub fn new_owned(message: ProtocolMessage) -> MessageWriter<'static> {
        MessageWriter {
            message: Cow::Owned(message),
            body_position: 0,
            body_len: 0,
            header_written: false,
        }
    }

    pub fn is_done(&self) -> bool {
        self.header_written && self.body_position >= self.body_len
    }

    pub fn write<T: Write>(&mut self, dest: &mut T) -> io::Result<()> {
        let MessageWriter {ref message, ref mut body_position, ref mut body_len, ref mut header_written} = *self;
        if !*header_written {
            let mut buffer = [0; BUFFER_LENGTH];
            let len = message.serialize(&mut buffer[..]);
            dest.write_all(&buffer[..len])?;
            *header_written = true;
        }

        if let Some(body) = message.get_body() {
            *body_len = body.len();
            while *body_position < *body_len {
                let to_write = &body[*body_position..];
                match dest.write(to_write) {
                    Ok(n) => *body_position += n,
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {} // ignore and retry
                    Err(ref e) if cfg!(target_os = "macos") && e.raw_os_error() == Some(41) => {
                        // osx is weird, and can sometimes return an EPROTOTYPE when writing
                        debug!(target: "eprototype", "Retrying write due to error: {:?}", e);
                    }
                    Err(other) => return Err(other)
                }
            }
        }
        Ok(())
    }
}

