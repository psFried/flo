mod flo_io;
mod channel_sender;
mod event_loops;

use futures::stream::Stream;
use tokio_core::net::{TcpStream, TcpListener};
use tokio_core::reactor::Interval;
use chrono::Duration;

use self::channel_sender::ChannelSender;
use self::engine::api::next_connection_id;
use server::engine::BackendChannels;
use event::ActorId;

use futures::sync::mpsc::unbounded;
use std::path::PathBuf;
use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use std::io;
use engine;

#[derive(Copy, Clone, PartialEq, Debug)]
#[allow(dead_code)]
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
    pub event_retention_duration: Duration,
    pub event_eviction_period: Duration,
    pub max_cached_events: usize,
    pub max_cache_memory: MemoryLimit,
    pub cluster_addresses: Option<Vec<SocketAddr>>,
    pub actor_id: ActorId,
    pub max_io_threads: Option<usize>,
}


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

        listener.incoming().map_err(|io_err| {
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

