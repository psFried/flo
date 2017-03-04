use std::io::{self, Write, Read};
use std::time::Duration;
use std::net::{TcpStream, ToSocketAddrs};

use nom::IResult;

use protocol::{ProtocolMessage, ClientProtocol, ClientProtocolImpl, MessageReader};

const BUFFER_LENGTH: usize = 8 * 1024;

pub trait IoStream: Read + Write {}
impl IoStream for TcpStream {}

pub struct ClientStream<T: IoStream> {
    io: MessageReader<T>,
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
            io: stream,
        }
    }
}

impl <T: IoStream> ClientStream<T> {

    pub fn write(&mut self, message: &mut ProtocolMessage) -> io::Result<()> {
        let mut buffer = [0; BUFFER_LENGTH];
        let nread = message.read(&mut buffer[..])?;
        self.io.write_all(&buffer[..nread])
    }

    pub fn write_event_data<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.write_all(data.as_ref()).and_then(|()| {
            self.writer.flush()
        })
    }

    pub fn read(&mut self) -> io::Result<ProtocolMessage> {
        self.io.read_next()
    }

}
