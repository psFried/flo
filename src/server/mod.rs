pub mod engine;
mod flo_io;

use futures::stream::Stream;
use futures::{Future};
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use tokio_core::reactor::Core;
use tokio_core::net::{TcpStream, TcpListener};
use tokio_core::io as nio;
use tokio_core::io::Io;

use protocol::{ClientProtocolImpl, ServerProtocolImpl};
use server::engine::api::{self, ClientMessage, ServerMessage, ClientConnect};
use server::flo_io::{ClientMessageStream, ServerMessageStream};
use std::path::PathBuf;
use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};


pub struct ServerOptions {
    pub port: u16,
    pub data_dir: PathBuf,
}

pub fn run(options: &ServerOptions) {
    let mut reactor = Core::new().unwrap();
    let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), options.port));
    let listener = TcpListener::bind(&address, &reactor.handle()).unwrap();

    let engine_sender = engine::run(options.data_dir.clone());

    info!("Started listening on port: {}", options.port);

    let clients = listener.incoming().map_err(|io_err| {
        format!("Error creating new connection: {:?}", io_err)
    });
    let future = clients.and_then(move |(tcp_stream, client_addr): (TcpStream, SocketAddr)| {
        debug!("Got new connection from: {:?}", client_addr);

        let (server_tx, server_rx): (UnboundedSender<ServerMessage>, UnboundedReceiver<ServerMessage>) = unbounded();

        let connection_id = api::next_connection_id();
        let (tcp_reader, tcp_writer) = tcp_stream.split();

        let engine_sender = engine_sender.clone();
        let client_connect = ClientConnect {
            connection_id: connection_id,
            client_addr: client_addr,
            message_sender: server_tx,
        };

        //TODO: don't unwrap this result, propegate errors instead
        engine_sender.send(ClientMessage::ClientConnect(client_connect)).unwrap();

        let client_stream = ClientMessageStream::new(connection_id, tcp_reader, ClientProtocolImpl);
        let client_to_server = client_stream.map_err(|err| {
            format!("Error parsing client stream: {:?}", err)
        }).and_then(move |client_message| {
            engine_sender.send(client_message).map_err(|err| {
                format!("Error sending message to backend server: {:?}", err)
            })
        });

        let server_to_client = nio::copy(ServerMessageStream::<ServerProtocolImpl>::new(connection_id, server_rx), tcp_writer).map_err(|err| {
            format!("Error writing to client: {:?}", err)
        });

        client_to_server.for_each(|inner_thing| {
            info!("for each inner thingy: {:?}", inner_thing);
            Ok(())
        }).join(server_to_client)

    }).for_each(|thingy| {
        info!("for_each thingy: {:?}", thingy);
        Ok(())
    });

    match reactor.run(future) {
        Ok(_) => info!("Gracefully stopped server"),
        Err(err) => error!("Error running server: {:?}", err)
    }
}

