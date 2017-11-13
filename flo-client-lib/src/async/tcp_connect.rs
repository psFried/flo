
use std::fmt::Debug;
use std::net::SocketAddr;

use futures::{Future, Poll};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;

use async::AsyncConnection;
use async::ops::HandshakeError;
use codec::EventCodec;


/// Connects to a server over TCP using the given options.
///
/// - `client_name`: Any arbitrary string used to identify the client to the server. At the moment, this is only really useful for debugging purposes, but future work will enable consumers with the same name to operate as a logical group
/// - `addr`: the socket address to use to connect to the server
/// - `consume_batch_size`: if this is `Some`, then the given batch size will be used for all consume operations. This number functions as an upper limit to the number of "in flight" events sent between the server and the client. If this argument is `None` then the default batch size for the server will be used
/// - `codec`: the `EventCodec` used to convert between the application events and the binary representation used by the server
/// - `handle`: a handle to a tokio event loop to use for establishing the connection
///
/// The returned future will resolve to an `AsyncConnection<D>` that is completely ready to use, with the handshake already complete.
///
/// # Example
///
/// ```no-run
/// tcp_connect_with("myClient", server_addr, None, StringCodec, &core.handle())
///         .and_then(|connection| {
///             // use connection
///             connection.produce_to(1, "/foo/bar", None, "event body".to_owned())
///         })
/// ```
pub fn tcp_connect_with<N: Into<String>, D: Debug + 'static, C: EventCodec<EventData=D> + 'static>(client_name: N, addr: &SocketAddr, consume_batch_size: Option<u32>, codec: C, handle: &Handle) -> AsyncTcpClientConnect<D> {
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
        }).and_then(move |connection| {
        connection.connect_with(consume_batch_size)
    });

    AsyncTcpClientConnect(Box::new(unboxed_future))
}

/// The same as `tcp_connect_with`, but always using the default `consume_batch_size`
pub fn tcp_connect<N: Into<String>, D: Debug + 'static, C: EventCodec<EventData=D> + 'static>(client_name: N, addr: &SocketAddr, codec: C, handle: &Handle) -> AsyncTcpClientConnect<D> {
    tcp_connect_with(client_name, addr, None, codec, handle)
}

/// a `Future` that resolves to an `AsyncConnection` that is ready to use.
#[must_use = "futures do nothing unless polled"]
pub struct AsyncTcpClientConnect<D: Debug>(Box<Future<Item=AsyncConnection<D>, Error=HandshakeError>>);

impl <D: Debug> Future for AsyncTcpClientConnect<D> {
    type Item = AsyncConnection<D>;
    type Error = HandshakeError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}




