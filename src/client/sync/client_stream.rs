use std::io::{self, Write, Read};
use std::time::Duration;
use std::net::{TcpStream, SocketAddr, ToSocketAddrs};

use nom::IResult;

use protocol::{ProtocolMessage, ServerMessage, EventHeader, read_server_message};
use flo_event::{FloEventId, FloEvent, OwnedFloEvent};
use super::{ClientError};


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
                reader.read(buf)?
            };
            trace!("read {} bytes", nread);
            self.len = nread;
            self.pos = 0;
        }
        let buf = &self.bytes[self.pos..self.len];
        debug!("Filled buffer: {:?}", buf);
        Ok(buf)
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

pub struct ClientStream<T: Write + Read> {
    writer: T,
    read_buffer: Buffer,
    op_id: u32
}

pub type SyncStream = ClientStream<TcpStream>;

impl SyncStream {
    pub fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<SyncStream> {
        TcpStream::connect(addr).and_then(|stream| {
            stream.set_read_timeout(Some(Duration::from_millis(1000))).map(|()| {
                SyncStream::from_stream(stream)
            })
        })
    }

    pub fn from_stream(stream: TcpStream) -> SyncStream {
        SyncStream {
            writer: stream,
            read_buffer: Buffer::new(),
            op_id: 1,
        }
    }
}

impl <T: Write + Read> ClientStream<T> {
    pub fn write(&mut self, message: &mut ProtocolMessage) -> io::Result<()> {
        let mut buffer = [0; BUFFER_LENGTH];
        let nread = message.read(&mut buffer[..])?;
        self.writer.write_all(&buffer[..nread])
    }


    pub fn read(&mut self) -> io::Result<ServerMessage<OwnedFloEvent>> {
        let ClientStream {ref mut writer, ref mut read_buffer, ..} = *self;

        let result = {
            let bytes = read_buffer.fill(writer)?;

            let result = read_server_message(bytes);
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

    pub fn produce<N: ToString, D: AsRef<[u8]>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.op_id += 1;
        let data = data.as_ref();
        let mut send_msg = ProtocolMessage::ProduceEvent(EventHeader{
            namespace: namespace.to_string(),
            op_id: self.op_id,
            data_length: data.len() as u32
        });

        let result = self.write(&mut send_msg).and_then(|()| {
            self.writer.write_all(data).and_then(|()| {
                self.writer.flush()
            })
        }).and_then(|()| {
            self.read()
        });

        match result {
            Ok(ServerMessage::EventPersisted(ref ack)) if ack.op_id == self.op_id => {
                Ok(ack.event_id)
            }
            Ok(other) => {
                let debug_msg = format!("{:?}", other);
                Err(ClientError::UnexpectedMessage(debug_msg))
            }
            Err(io_err) => {
                Err(io_err.into())
            }
        }
    }

}
