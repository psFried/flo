use std::io::{self, Write, Read};
use std::time::Duration;
use std::net::{TcpStream, SocketAddr, ToSocketAddrs};

use nom::IResult;

use protocol::{ProtocolMessage, ServerMessage, ProduceEventHeader, ClientProtocol, ClientProtocolImpl};
use flo_event::{FloEventId, FloEvent, OwnedFloEvent};
use client::{ClientError};


const BUFFER_LENGTH: usize = 8 * 1024;

struct Buffer {
    bytes: Vec<u8>,
    pos: usize,
    len: usize,
}

impl Buffer {
    fn new() -> Buffer {
        Buffer {
            bytes: vec![0; BUFFER_LENGTH],
            pos: 0,
            len: 0
        }
    }

    fn fill<R: Read>(&mut self, reader: &mut R) -> io::Result<&[u8]> {
        if self.pos >= self.len {
            let nread = {
                let mut buf = &mut self.bytes[..];
                let mut nread = 0;
                loop {
                    match reader.read(buf) {
                        Ok(n) => {
                            nread = n;
                            break;
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                            trace!("Interrupted reading from socket, trying again");
                        },
                        Err(io_err) => {
                            return Err(io_err);
                        }
                    }
                }
                nread
            };
            trace!("read {} bytes", nread);
            self.len = nread;
            self.pos = 0;
        }
        let buf = &self.bytes[self.pos..self.len];
        debug!("Returning buffer: {:?}", buf);
        Ok(buf)
    }

    fn drain(&mut self, num_bytes: usize) -> &[u8] {
        let pos = self.pos;
        let byte_count = ::std::cmp::min(num_bytes, self.len - pos);
        self.consume(byte_count);
        &self.bytes[pos..(pos + byte_count)]
    }

    fn consume(&mut self, nbytes: usize) {
        self.pos += nbytes;
    }
}

impl ::std::ops::Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.bytes[self.pos..self.len]
    }
}


pub trait IoStream: Read + Write {}
impl IoStream for TcpStream {}

pub struct ClientStream<T: IoStream> {
    writer: T,
    read_buffer: Buffer,
}

pub type SyncStream = ClientStream<TcpStream>;

impl SyncStream {
    pub fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<SyncStream> {
        TcpStream::connect(addr).and_then(|stream| {
            stream.set_read_timeout(Some(Duration::from_millis(10_000))).and_then(|()| {
                stream.set_nonblocking(false).map(|()| {
                    SyncStream::from_stream(stream)
                })
            })
        })
    }

    pub fn from_stream(stream: TcpStream) -> SyncStream {
        SyncStream {
            writer: stream,
            read_buffer: Buffer::new(),
        }
    }
}

impl <T: IoStream> ClientStream<T> {

    pub fn write(&mut self, message: &mut ProtocolMessage) -> io::Result<()> {
        let mut buffer = [0; BUFFER_LENGTH];
        let nread = message.read(&mut buffer[..])?;
        self.writer.write_all(&buffer[..nread])
    }

    pub fn write_event_data<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.write_all(data.as_ref()).and_then(|()| {
            self.writer.flush()
        })
    }

    pub fn read(&mut self) -> io::Result<ProtocolMessage> {
        let ClientStream {ref mut writer, ref mut read_buffer, ..} = *self;

        let result = {
            let bytes = read_buffer.fill(writer)?;
            let protocol = ClientProtocolImpl;
            let result = protocol.parse_any(bytes);
            match result {
                IResult::Done(remaining, message) => Ok((bytes.len() - remaining.len(), message)),
                IResult::Incomplete(needed) => {
                    //TODO: change the way we do this to allow receiving arbitrarily large messages
                    Err(io::Error::new(io::ErrorKind::InvalidData, format!("Insufficient data to deserialize message: {:?}", needed)))
                }
                IResult::Error(err) => {
                    Err(io::Error::new(io::ErrorKind::InvalidData, format!("Error deserializing message: {:?}", err)))
                }
            }
        };

        result.map(|(consumed, message)| {
            read_buffer.consume(consumed);
            message
        })
    }

    pub fn read_event_data(&mut self, data_len: usize) -> io::Result<Vec<u8>> {
        let existing_data = self.read_buffer.drain(data_len);

        let mut data = Vec::with_capacity(data_len);
        data.extend_from_slice(existing_data);

        let position = existing_data.len();
        if position < data_len {
            unsafe {
                data.set_len(data_len);
            }
            let buffer = &mut data[position..];
            self.writer.read_exact(buffer)?
        }

        Ok(data)
    }
}
