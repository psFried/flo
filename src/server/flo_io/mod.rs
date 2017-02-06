mod client_message_stream;
mod server_message_stream;

use std::net::SocketAddr;

pub use self::client_message_stream::ClientMessageStream;
pub use self::server_message_stream::ServerMessageStream;
use server::channel_sender::ChannelSender;
use protocol::{ClientProtocolImpl, ServerProtocolImpl, ServerMessage};
use server::engine::api::{self, ProducerMessage, ConsumerMessage, ConnectionId, ClientMessage, ClientConnect};

use tokio_core::reactor::Remote;
use tokio_core::net::TcpStream;
use tokio_core::io::Io;
use tokio_core::io as nio;
use futures::sync::mpsc::{unbounded, UnboundedSender, UnboundedReceiver};
use futures::{Future, Stream};

pub fn setup_message_streams(tcp_stream: TcpStream, client_addr: SocketAddr, mut engine: ChannelSender, remote_handle: &Remote) {

    remote_handle.spawn(move |_handle| {
        let connection_id = api::next_connection_id();
        debug!("Established new connection to: {:?} as connection_id: {}", client_addr, connection_id);
        let (server_tx, server_rx): (UnboundedSender<ServerMessage>, UnboundedReceiver<ServerMessage>) = unbounded();
        let (tcp_reader, tcp_writer) = tcp_stream.split();

        send_client_connect(&mut engine, connection_id, client_addr, &server_tx);

        let client_stream = ClientMessageStream::new(connection_id, tcp_reader, ClientProtocolImpl);
        let client_to_server = client_stream.map_err(|err| {
            format!("Error parsing client stream: {:?}", err)
        }).and_then(move |client_message| {
            let log = format!("Sent message: {:?}", client_message);
            engine.send(client_message).map_err(|err| {
                format!("Error sending message: {:?}", err)
            }).map(|()| {
                log
            })
        }).for_each(|inner_thing| {
            info!("for each inner thingy: {:?}", inner_thing);
            Ok(())
        }).or_else(|err| {
            warn!("Recovering from error: {:?}", err);
            Ok(())
        });

        let server_to_client = nio::copy(ServerMessageStream::<ServerProtocolImpl>::new(connection_id, server_rx), tcp_writer).map_err(|err| {
            error!("Error writing to client: {:?}", err);
            format!("Error writing to client: {:?}", err)
        }).map(move |amount| {
            info!("Wrote: {} bytes to client: {:?}, connection_id: {}, dropping connection", amount, client_addr, connection_id);
            ()
        });

        client_to_server.select(server_to_client).then(move |res| {
            match res {
                Ok((compl, _fut)) => {
                    info!("Finished with connection: {}, value: {:?}", connection_id, compl);
                }
                Err((err, _)) => {
                    warn!("Error with connection: {}, err: {:?}", connection_id, err);
                }
            }
            Ok(())
        })
    });
}

fn send_client_connect(engine: &mut ChannelSender, connection_id: ConnectionId, client_address: SocketAddr, sender: &UnboundedSender<ServerMessage>) {
    let client_connect = ClientConnect {
        connection_id: connection_id,
        client_addr: client_address,
        message_sender: sender.clone(),
    };
    engine.send(ClientMessage::Both(
        ConsumerMessage::ClientConnect(client_connect.clone()),
        ProducerMessage::ClientConnect(client_connect)
    )).unwrap(); //TODO: something better than unwrapping this result
}
