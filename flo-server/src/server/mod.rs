mod flo_io;
mod channel_sender;
mod server_options;

use futures::sync::mpsc::unbounded;
use futures::{Stream, Sink, Future};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_core::reactor::Interval;

use self::channel_sender::ChannelSender;
use self::engine::api::next_connection_id;
use server::engine::BackendChannels;
use event_loops;

use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use std::io;
use engine;

pub use self::server_options::{ServerOptions, MemoryLimit, MemoryUnit};


pub fn run(options: ServerOptions) -> io::Result<()> {
    if options.use_new_engine {
        run_new_engine(options)
    } else {
        run_old_engine(options)
    }
}

fn run_new_engine(options: ServerOptions) -> io::Result<()> {
    #[allow(deprecated)]
    use tokio_core::io::Io;
    use new_engine::{ControllerOptions,
                     start_controller,
                     system_stream_name,
                     create_client_channels,
                     ConnectionHandler};
    use new_engine::event_stream::EventStreamOptions;
    use self::flo_io::{ProtocolMessageStream, ServerMessageStream};

    const ONE_GB: usize = 1024 * 1024 * 1024;

    let (join_handle, mut event_loop_handles) = event_loops::spawn_event_loop_threads(options.max_io_threads).unwrap();

    let controller_options = ControllerOptions {
        storage_dir: options.data_dir.clone(),
        default_stream_options: EventStreamOptions{
            name: system_stream_name(),
            num_partitions: 1,
            event_retention: options.event_retention_duration,
            max_segment_duration: options.event_eviction_period,
            segment_max_size_bytes: ONE_GB,
        },
    };

    let engine_ref = start_controller(controller_options, event_loop_handles.next_handle())?;

    let server_port = options.port;
    let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), server_port));
    let listener = ::std::net::TcpListener::bind(address)?;

    let remote = event_loop_handles.next_handle();
    // use the same remote for the connection handler so that all the io for a given connection is on the same thread
    remote.spawn(move |handle| {

        let listener = TcpListener::from_listener(listener, &address, &handle).unwrap();

        info!("Started listening on port: {}", server_port);

        let incoming = listener.incoming();
        incoming.map_err(|io_err| {
            error!("Error creating new connection: {:?}", io_err);
        }).for_each(move |(tcp_stream, client_addr): (TcpStream, SocketAddr)| {
            tcp_stream.set_nodelay(true).map_err(|io_err| {
                error!("Error setting NODELAY. Nagle yet lives!: {:?}", io_err);
                ()
            })?;
            let client_engine_ref = engine_ref.clone();
            let connection_id = client_engine_ref.next_connection_id();
            let remote_handle = event_loop_handles.next_handle();

            let (client_tx, client_rx) = create_client_channels();

            info!("Opened connection_id: {} to address: {}", connection_id, client_addr);

            remote_handle.spawn(move |client_handle| {

                #[allow(deprecated)]
                let (tcp_reader, tcp_writer) = tcp_stream.split();

                let server_to_client = ServerMessageStream::new(connection_id, client_rx, tcp_writer);

                let client_message_stream = ProtocolMessageStream::new(connection_id, tcp_reader);
                let connection_handler = ConnectionHandler::new(
                    connection_id,
                    client_tx.clone(),
                    client_engine_ref,
                     client_handle.clone());

                let client_to_server = connection_handler
                        .send_all(client_message_stream)
                        .map(|_| ());

                client_to_server.select(server_to_client).then(move |res| {
                    if let Err((err, _)) = res {
                        warn!("Closing connection: {} due to err: {:?}", connection_id, err);
                    }
                    info!("Closed connection_id: {} to address: {}", connection_id, client_addr);
                    Ok(())
                })

            });

            Ok(())
        })
    });

    join_handle.join();
    Ok(())
}


fn run_old_engine(options: ServerOptions) -> io::Result<()> {
    let server_port = options.port;
    let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), server_port));

    let listener = ::std::net::TcpListener::bind(address)?;

    let (join_handle, mut event_loop_handles) = event_loops::spawn_event_loop_threads(options.max_io_threads).unwrap();

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

