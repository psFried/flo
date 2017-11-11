
use std::fmt::Debug;
use std::net::SocketAddr;

use futures::{Future, Poll};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;

use async::AsyncConnection;
use async::ops::HandshakeError;
use codec::EventCodec;



pub fn tcp_connect<N: Into<String>, D: Debug + 'static, C: EventCodec<EventData=D> + 'static>(client_name: N, addr: &SocketAddr, codec: C, handle: &Handle) -> AsyncTcpClientConnect<D> {
    let client_name = client_name.into();
    let boxed_codec = Box::new(codec) as Box<EventCodec<EventData=D>>;

    let unboxed_future = TcpStream::connect(addr, handle)
            .and_then(|tcp| {
                // Kill Nagle, always. Performance goes to complete shit if it's enabled, and we're decently good about sending most messages in one write call
                tcp.set_nodelay(true).map(|()| tcp)
            })
            .map(|tcp| {
                AsyncConnection::from_tcp_stream(client_name, tcp, boxed_codec)
            })
            .map_err(|io_err| {

                HandshakeError {
                    message: "Failed to create connection from TCP stream",
                    error_type: io_err.into(),
                }
            }).and_then(|connection| {
        connection.connect()
    });

    AsyncTcpClientConnect(Box::new(unboxed_future))
}


pub struct AsyncTcpClientConnect<D: Debug>(Box<Future<Item=AsyncConnection<D>, Error=HandshakeError>>);

impl <D: Debug> Future for AsyncTcpClientConnect<D> {
    type Item = AsyncConnection<D>;
    type Error = HandshakeError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}




