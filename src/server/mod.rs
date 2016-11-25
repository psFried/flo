mod buffer_pool;
pub mod engine;
mod flo_io;

use futures::stream::Stream;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpStream, TcpStreamNew, TcpListener, Incoming};
use tokio_core::io as nio;
use self::buffer_pool::BufferPool;
use protocol;
use event::Event;

use std::path::PathBuf;
use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use std::io::Write;


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

    let clients: Incoming = listener.incoming();
    let future = clients.and_then(move |(tcp_stream, client_addr): (TcpStream, SocketAddr)| {
        debug!("Got new connection from: {:?}", client_addr);

        //TODO: accept client connections
        /*
        try to parse out a RequestHeader from the TcpStream
        if it's error, then write some generic error message and just return an error
        if it a success:
            create a new futures::sync::mpsc::{UnboundedSender, UnboundedReceiver} pair
            create a NewClient message with the UnboundedSender. Used for the backend engine to send messages to the io layer
            send the NewClient message to the backend engine via the engine_sender channel

            split the TcpStream into ReadHalf and WriteHalf

            let read_future = convert the ReadHalf into a Stream<ClientMessage> anf for_each:
                send the ClientMessage to the backend engine via the engine_sender channel

            let write_future = for_each on the UnboundedReceiver:
                serialize each message and write it to the WriteHalf

            return joined read_future and write_future
        */



        nio::write_all(tcp_stream, b"Hello World!\n")
    }).for_each(|(socket, buffer): (TcpStream, &[u8; 13])| {
        if let Ok(str_val) = ::std::str::from_utf8(buffer) {
            debug!("Successfully wrote to socket: {:?}: {:?}", socket, str_val);
        } else {
            debug!("Successfully wrote to socket: {:?}: {:?}", socket, buffer);
        }
        Ok(())
    });

    match reactor.run(future) {
        Ok(_) => info!("Gracefully stopped server"),
        Err(err) => error!("Error running server: {:?}", err)
    }
}

