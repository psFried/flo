pub mod engine;
mod flo_io;
mod channel_sender;

use futures::stream::Stream;
use futures::{Future};
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use tokio_core::reactor::Core;
use tokio_core::net::{TcpStream, TcpListener};
use tokio_core::io as nio;
use tokio_core::io::Io;

use flo_event::OwnedFloEvent;
use self::channel_sender::ChannelSender;
use protocol::{ClientProtocolImpl, ServerProtocolImpl};
use server::engine::api::{self, ClientMessage, ProducerMessage, ConsumerMessage, ClientConnect};
use protocol::ServerMessage;
use server::flo_io::{ClientMessageStream, ServerMessageStream};
use server::engine::BackendChannels;

use std::path::PathBuf;
use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum MemoryUnit {
    Megabyte,
    Kilobyte,
    Byte
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct MemoryLimit {
    amount: usize,
    unit: MemoryUnit,
}


impl MemoryLimit {
    pub fn new(amount: usize, unit: MemoryUnit) -> MemoryLimit {
        MemoryLimit {
            amount: amount,
            unit: unit,
        }
    }

    pub fn as_bytes(&self) -> usize {
        let multiplier = match self.unit {
            MemoryUnit::Byte => 1,
            MemoryUnit::Kilobyte => 1024,
            MemoryUnit::Megabyte => 1024 * 1024,
        };
        multiplier * self.amount
    }
}

#[derive(PartialEq, Clone)]
pub struct ServerOptions {
    pub port: u16,
    pub data_dir: PathBuf,
    pub default_namespace: String,
    pub max_events: usize,
    pub max_cached_events: usize,
    pub max_cache_memory: MemoryLimit,
}

pub fn run(options: ServerOptions) {
    let mut reactor = Core::new().unwrap();
    let server_port = options.port;
    let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), server_port));
    let loop_handle = reactor.handle();
    let listener = TcpListener::bind(&address, &loop_handle).unwrap();

    let BackendChannels{producer_manager, consumer_manager} = engine::run(options);

    info!("Started listening on port: {}", server_port);

    let incoming = listener.incoming().map_err(|io_err| {
        format!("Error creating new connection: {:?}", io_err)
    }).for_each(move |(tcp_stream, client_addr): (TcpStream, SocketAddr)| {
        debug!("Got new connection from: {:?}", client_addr);

        let (server_tx, server_rx): (UnboundedSender<ServerMessage<Arc<OwnedFloEvent>>>, UnboundedReceiver<ServerMessage<Arc<OwnedFloEvent>>>) = unbounded();

        let connection_id = api::next_connection_id();
        let (tcp_reader, tcp_writer) = tcp_stream.split();

        let channel_sender = ChannelSender {
            producer_manager: producer_manager.clone(),
            consumer_manager: consumer_manager.clone(),
        };

        let client_connect = ClientConnect {
            connection_id: connection_id,
            client_addr: client_addr,
            message_sender: server_tx.clone(),
        };

        channel_sender.send(ClientMessage::Both(
            ConsumerMessage::ClientConnect(client_connect.clone()),
            ProducerMessage::ClientConnect(client_connect)
        )).unwrap(); //TODO: something better than unwrapping this result

        let client_stream = ClientMessageStream::new(connection_id, tcp_reader, ClientProtocolImpl);
        let client_to_server = client_stream.map_err(|err| {
            format!("Error parsing client stream: {:?}", err)
        }).and_then(move |client_message| {
            let log = format!("Sent message: {:?}", client_message);
            channel_sender.send(client_message).map_err(|err| {
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

        let server_to_client = nio::copy(ServerMessageStream::<ServerProtocolImpl<Arc<OwnedFloEvent>>>::new(connection_id, server_rx), tcp_writer).map_err(|err| {
            error!("Error writing to client: {:?}", err);
            format!("Error writing to client: {:?}", err)
        }).map(move |amount| {
            info!("Wrote: {} bytes to client: {:?}, connection_id: {}, dropping connection", amount, client_addr, connection_id);
            ()
        });

        let future = client_to_server.select(server_to_client).then(move |res| {
            match res {
                Ok((compl, _fut)) => {
                    info!("Finished with connection: {}, value: {:?}", connection_id, compl);
                }
                Err((err, _)) => {
                    warn!("Error with connection: {}, err: {:?}", connection_id, err);
                }
            }
            Ok(())
        });
        loop_handle.spawn(future);

        Ok(())
    });

    match reactor.run(incoming) {
        Ok(_) => info!("Gracefully stopped server"),
        Err(err) => error!("Error running server: {:?}", err)
    }
}

