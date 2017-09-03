mod flo_io;
mod channel_sender;
mod event_loops;
mod server_options;

use futures::stream::Stream;
use tokio_core::net::{TcpStream, TcpListener};
use tokio_core::reactor::Interval;

use self::channel_sender::ChannelSender;
use self::engine::api::next_connection_id;
use server::engine::BackendChannels;

use futures::sync::mpsc::unbounded;
use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use std::io;
use engine;

pub use self::server_options::{ServerOptions, MemoryLimit, MemoryUnit};


pub fn run(options: ServerOptions) -> io::Result<()> {

    let server_port = options.port;
    let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), server_port));

    let listener = ::std::net::TcpListener::bind(address)?;

    let (join_handle, mut event_loop_handles) = self::event_loops::spawn_event_loop_threads(options.max_io_threads).unwrap();

    let (cluster_tx, cluster_rx) = unbounded();

    let BackendChannels{producer_manager, consumer_manager} = engine::run(options, cluster_tx);

    let mut client_loop_handles = event_loop_handles.clone();
    let channel_sender = ChannelSender {
        producer_manager: producer_manager.clone(),
        consumer_manager: consumer_manager.clone(),
    };

    client_loop_handles.next_handle().spawn(move |handle| {

        let listener = TcpListener::from_listener(listener, &address, &handle).unwrap();

        info!("Started listening on port: {}", server_port);

        let incoming = listener.incoming();
        incoming.map_err(|io_err| {
            error!("Error creating new connection: {:?}", io_err);
        }).for_each(move |(tcp_stream, client_addr): (TcpStream, SocketAddr)| {

            let connection_id = next_connection_id();
            let remote_handle = client_loop_handles.next_handle();
            flo_io::setup_message_streams(connection_id, tcp_stream, client_addr, channel_sender.clone(), &remote_handle);
            Ok(())
        })
    });

    let engine_sender = ChannelSender {
        producer_manager: producer_manager.clone(),
        consumer_manager: consumer_manager.clone(),
    };
    flo_io::start_cluster_io(cluster_rx, event_loop_handles.clone(), engine_sender);

    event_loop_handles.next_handle().spawn(move |handle| {
        let channel =producer_manager.clone();
        let interval = Interval::new_at(::std::time::Instant::now(), engine::tick_duration(), handle).unwrap();
        interval.map_err(|err| {
            error!("Failed to start engine tick interval: {}", err);
        }).for_each(move |_| {
            channel.send(::engine::api::ProducerManagerMessage::Tick).map_err(|send_err| {
                error!("Failed to send engine tick message: {}", send_err);
            })
        })
    });

    join_handle.join();

    Ok(())
}

