use std::io::{self, Write, Read};
use std::time::Duration;
use std::net::{TcpStream, ToSocketAddrs};

use protocol::{ProtocolMessage, MessageStream, MessageWriter};
use sync::Transport;

/// Trait to mark anything that can be used as a Transport for the connection. Currently, this is just `TcpStream`.
pub trait IoStream: Read + Write {}
impl IoStream for TcpStream {}

/// A `ClientStream` sends and receives messages over an `IoStream`. Users should typically just
/// use the `SyncStream` type alias instead of using `ClientStream` directly.
pub struct ClientStream<T: IoStream> {
    io: MessageStream<T>,
}

/// The default `Transport` impl. Users can use this to easily create a `Trasport` impl that uses a `TcpStream` to handle
/// communication with the flo server
pub type SyncStream = ClientStream<TcpStream>;

impl SyncStream {

    /// Connects to the flo server at the specified address, using the default TCP options and a 10 second timeout
    pub fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<SyncStream> {
        TcpStream::connect(addr).and_then(|stream| {
            stream.set_read_timeout(Some(Duration::from_millis(10_000))).and_then(|()| {
                stream.set_nonblocking(false).map(|()| {
                    SyncStream::from_stream(stream)
                })
            })
        })
    }

    /// Creates a `SyncStream` from a `TcpStream` that is already connected. This allows for setting any
    /// tcp options. This option is likely to be removed in a future release, though, as automatic reconnection features
    /// are added.
    pub fn from_stream(stream: TcpStream) -> SyncStream {
        SyncStream {
            io: MessageStream::new(stream),
        }
    }
}

impl <T: IoStream> Transport for ClientStream<T> {
    fn send(&mut self, mut message: ProtocolMessage) -> io::Result<()> {
        let mut writer = MessageWriter::new(&mut message);
        self.io.write(&mut writer)
    }

    fn receive(&mut self) -> io::Result<ProtocolMessage> {
        self.io.read_next()
    }
}
