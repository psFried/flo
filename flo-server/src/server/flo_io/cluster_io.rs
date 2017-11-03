use std::net::SocketAddr;

use event_loops::LoopHandles;
use server::engine::api::{next_connection_id, ProducerManagerMessage, ClientMessage};
use super::setup_message_streams;
use server::channel_sender::ChannelSender;

use tokio_core::reactor::{Remote, Handle};
use tokio_core::net::{TcpStream, TcpStreamNew};
use futures::{Future, Poll, Async, Stream};
use futures::sync::mpsc::UnboundedReceiver;

pub struct ConnectFuture {
    create: TcpStreamNew,
    remote: Remote,
    engine: Option<ChannelSender>,
    address: SocketAddr,
}

impl Future for ConnectFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.create.poll() {
            Ok(Async::Ready(tcp_stream)) => {
                let connection_id = next_connection_id();
                debug!("established connection to peer at: {} with connection_id: {}", self.address, connection_id);

                let engine = self.engine.take().unwrap();
                setup_message_streams(connection_id, tcp_stream, self.address, engine, &self.remote);
                Ok(Async::Ready(()))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(io_err) => {
                info!("Failed to connect to peer address: {:?}, err: {}", self.address, io_err);
                let fail = ProducerManagerMessage::OutgoingConnectFailure(self.address);
                let engine = self.engine.take().unwrap();
                engine.send(ClientMessage::Producer(fail)).expect("Failed to send PeerConnectFailed message to producer manager");
                Err(())
            }
        }
    }
}

fn connect(address: SocketAddr, handle: &Handle, remote: Remote, engine: ChannelSender) -> ConnectFuture {
    let create_stream = TcpStream::connect(&address, handle);
    ConnectFuture {
        address,
        remote,
        create: create_stream,
        engine: Some(engine),
    }
}

pub fn start_cluster_io(receiver: UnboundedReceiver<SocketAddr>, mut loop_handles: LoopHandles, engine: ChannelSender) {
    loop_handles.next_handle().spawn(move |_| {
        receiver.for_each(move |peer_address| {
            let engine = engine.clone();
            let remote = loop_handles.next_handle();
            let address = peer_address.clone();
            loop_handles.next_handle().spawn(move |handle| {
                connect(address, handle, remote, engine)
            });
            Ok(())
        })
    });
}
