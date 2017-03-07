mod client_message_stream;
mod server_message_stream;
mod cluster_io;

use std::net::SocketAddr;
use std::sync::atomic;

pub use self::client_message_stream::ClientMessageStream;
pub use self::server_message_stream::ServerMessageStream;
use server::channel_sender::ChannelSender;
use protocol::{ServerProtocolImpl, ServerMessage};
use server::engine::api::{ConnectionId, ClientMessage, ClientConnect};

use tokio_core::reactor::Remote;
use tokio_core::net::TcpStream;
use tokio_core::io::Io;
use tokio_core::io as nio;
use futures::sync::mpsc::{unbounded, UnboundedSender, UnboundedReceiver};
use futures::{Future, Stream};

pub use self::cluster_io::start_cluster_io;

static OPEN_CONNECTION_COUNT: atomic::AtomicUsize = atomic::ATOMIC_USIZE_INIT;

fn connection_opened() -> usize {
    //fetch_add returns the previous value, so we need to add one to it
    OPEN_CONNECTION_COUNT.fetch_add(1, atomic::Ordering::SeqCst) + 1
}

fn connection_closed() -> usize {
    //fetch_sub returns the previous value, so we need to subtract one from it to get the current
    OPEN_CONNECTION_COUNT.fetch_sub(1, atomic::Ordering::SeqCst) - 1
}


pub fn setup_message_streams(connection_id: ConnectionId, tcp_stream: TcpStream, client_addr: SocketAddr, mut engine: ChannelSender, remote_handle: &Remote) {

    remote_handle.spawn(move |_handle| {
        let current_connection_count = connection_opened();
        info!("Established new connection to: {} as connection_id: {}, total active connections: {}", client_addr, connection_id, current_connection_count);
        let (server_tx, server_rx): (UnboundedSender<ServerMessage>, UnboundedReceiver<ServerMessage>) = unbounded();
        let (tcp_reader, tcp_writer) = tcp_stream.split();

        send_client_connect(&mut engine, connection_id, client_addr.clone(), &server_tx);

        let client_stream = ClientMessageStream::new(connection_id, client_addr, tcp_reader);
        let client_to_server = client_stream.map_err(|err| {
            format!("Error parsing client stream: {:?}", err)
        }).for_each(move |client_message| {
            engine.send(client_message).map_err(|err| {
                format!("Error sending message: {:?}", err)
            })
        }).or_else(|err| {
            warn!("Recovering from error: {:?}", err);
            Ok(())
        });

        let server_to_client = nio::copy(ServerMessageStream::<ServerProtocolImpl>::new(connection_id, server_rx), tcp_writer).map_err(|err| {
            error!("Error writing to client: {:?}", err);
            format!("Error writing to client: {:?}", err)
        }).map(move |amount| {
            info!("Wrote: {} bytes to connection_id: {}, dropping connection", amount, connection_id);
            ()
        });

        client_to_server.select(server_to_client).then(move |res| {
            if let Err((err, _)) = res {
                warn!("Closing connection: {} due to err: {:?}", connection_id, err);
            }
            let current_connection_count = connection_closed();
            info!("Closed connection_id: {} to address: {}, total active connections: {}", connection_id, client_addr, current_connection_count);
            Ok(())
        })
    });
}

fn send_client_connect(engine: &mut ChannelSender, connection_id: ConnectionId, client_address: SocketAddr, sender: &UnboundedSender<ServerMessage>) {
    engine.send(ClientMessage::connect(connection_id, client_address, sender.clone())).unwrap(); //TODO: something better than unwrapping this result
}
