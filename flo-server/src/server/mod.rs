mod server_options;


use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use std::io;
use tokio_core::reactor::Remote;
use tokio_core::net::{TcpStream, TcpListener};
use futures::{Stream, Future};

use engine::{ControllerOptions,
             EngineRef,
             ClusterOptions,
             start_controller,
             system_stream_name};
use engine::connection_handler::create_connection_control_channels;
use engine::controller::ConnectionRef;
use engine::event_stream::EventStreamOptions;
use flo_io::create_connection_handler;
use event_loops;

pub use self::server_options::{ServerOptions, MemoryLimit, MemoryUnit};

const ONE_GB: usize = 1024 * 1024 * 1024;

pub fn run(mut options: ServerOptions) -> io::Result<()> {

    let (join_handle, mut event_loop_handles) = event_loops::spawn_event_loop_threads(options.max_io_threads).unwrap();

    let this_address = options.this_instance_address.take();
    let peer_addresses = options.cluster_addresses.take();

    let cluster_options = this_address.map(|server_addr| {
        let peers = peer_addresses.unwrap();
        ClusterOptions {
            election_timeout_millis: options.election_timeout_millis,
            heartbeat_interval_millis: options.heartbeat_interval_millis,
            this_instance_address: server_addr,
            peer_addresses: peers,
            event_loop_handles: event_loop_handles.clone(),
        }
    });

    let controller_options = ControllerOptions {
        storage_dir: options.data_dir.clone(),
        default_stream_options: EventStreamOptions{
            name: system_stream_name(),
            num_partitions: 1,
            event_retention: options.event_retention_duration,
            max_segment_duration: options.event_eviction_period,
            segment_max_size_bytes: ONE_GB,
        },
        cluster_options,
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

            handle_incoming_connection(&engine_ref, event_loop_handles.next_handle(), tcp_stream, client_addr)
        })
    });

    join_handle.join();
    Ok(())
}

fn handle_incoming_connection(engine_ref: &EngineRef, remote_handle: Remote, tcp_stream: TcpStream, client_addr: SocketAddr) -> Result<(), ()> {
    tcp_stream.set_nodelay(true).map_err(|io_err| {
        error!("Error setting NODELAY. Nagle yet lives!: {:?}", io_err);
        ()
    })?;

    let mut client_engine_ref = engine_ref.clone();
    let connection_id = client_engine_ref.next_connection_id();
    let (control_tx, control_rx) = create_connection_control_channels();
    let connection_ref = ConnectionRef {
        connection_id,
        remote_address: client_addr,
        control_sender: control_tx,
    };
    client_engine_ref.system_stream().incoming_connection_accepted(connection_ref);

    remote_handle.spawn(move |client_handle| {
        create_connection_handler(client_handle.clone(), client_engine_ref, connection_id, client_addr, tcp_stream, control_rx)
    });

    Ok(())
}

