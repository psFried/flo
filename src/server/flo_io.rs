use futures::{Future, Poll, Async};
use futures::stream::Stream;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpStream, TcpStreamNew, TcpListener, Incoming};
use tokio_core::io as nio;
use tokio_core::io::Io;
use std::io::{Read, Write, BufRead};
use std::sync::mpsc::Sender;

use server::engine::api::{self, ClientMessage, NewClient, ConnectionId};
use protocol::{ClientProtocol};
use nom::IResult;

pub struct ServerMessageStream {
    connection_id: ConnectionId,
    tcp_writer: nio::WriteHalf<TcpStream>,
}

pub struct ClientMessageStream<R: Read, P: ClientProtocol> {
    connection_id: ConnectionId,
    tcp_reader: R,
    protocol: P,
    buffer: Vec<u8>,
    buffer_pos: usize,
}

impl <R: Read, P: ClientProtocol> Stream for ClientMessageStream<R, P> {
    type Item = ClientMessage;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let ClientMessageStream {
            ref connection_id,
            ref mut tcp_reader,
            ref mut buffer,
            ref mut buffer_pos,
            ref protocol,
        } = *self;

        let nread = match tcp_reader.read(buffer) {
            Ok(amount) => amount,
            Err(ref err) if err.kind() == ::std::io::ErrorKind::WouldBlock => {
                return Ok(Async::NotReady);
            },
            Err(err) => {
                return Err(format!("Error reading from stream: {:?}", err));
            }
        };

        Ok(Async::Ready(None))
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use futures::{Future, Poll, Async};
    use futures::stream::Stream;
    use tokio_core::reactor::Core;
    use tokio_core::net::{TcpStream, TcpStreamNew, TcpListener, Incoming};
    use tokio_core::io as nio;
    use tokio_core::io::Io;
    use std::io::{Read, Write, BufRead};
    use std::sync::mpsc::Sender;

    use server::engine::api::{self, ClientMessage, NewClient, ConnectionId};
    use protocol::{ClientProtocol, ClientProtocolImpl, ProduceEvent, RequestHeader};
    use nom::IResult;

    #[test]
    fn poll_returns_message_header_when_read_and_parse_both_succeed() {
        struct Proto;
        impl ClientProtocol for Proto {
            fn parse_header<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], RequestHeader> {
                unimplemented!()
            }

            fn parse_producer_event<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProduceEvent> {
                unimplemented!()
            }
        }
        let mut subject = ClientMessageStream {
            connection_id: 0,
            tcp_reader: WouldBlockRead,
            buffer: vec![0, 64],
            buffer_pos: 0,
            protocol: Proto
        };

    }

    #[test]
    fn poll_returns_not_ready_when_reader_would_block() {
        let mut subject = ClientMessageStream {
            connection_id: 0,
            tcp_reader: WouldBlockRead,
            protocol: FailProtocol,
            buffer: vec![0, 64],
            buffer_pos: 0
        };

        let result = subject.poll();
        assert_eq!(Ok(Async::NotReady), result);
    }

    struct WouldBlockRead;
    impl Read for WouldBlockRead {
        fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
            Err(::std::io::Error::new(::std::io::ErrorKind::WouldBlock, "would_block_err"))
        }
    }

    struct FailProtocol;
    impl ClientProtocol for FailProtocol {
        fn parse_header<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], RequestHeader> {
            unimplemented!()
        }

        fn parse_producer_event<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProduceEvent> {
            unimplemented!()
        }
    }

}




