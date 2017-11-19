mod flo_io;
mod server_options;

use futures::{Stream, Sink, Future};
use tokio_core::net::{TcpStream, TcpListener};

use event_loops;

use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use std::io;

pub use self::server_options::{ServerOptions, MemoryLimit, MemoryUnit};



pub fn run(options: ServerOptions) -> io::Result<()> {
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

