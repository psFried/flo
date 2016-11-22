
use futures::stream::Stream;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpStream, TcpStreamNew, TcpListener};
use tokio_core::io as nio;

use std::path::PathBuf;
use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use std::io::Write;

pub struct ServerOptions {
    pub port: u16,
    pub data_dir: PathBuf,
}

pub fn run(options: &ServerOptions) {
    let mut reactor = Core::new().unwrap();
    //unwraps are safe here since this
    let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), options.port));

    let listener = TcpListener::bind(&address, &reactor.handle()).unwrap();

    info!("Started listening on port: {}", options.port);

    let clients = listener.incoming();

    let future = clients.and_then(|(socket, client_addr)| {
        debug!("Got new connection from: {:?}", client_addr);
        nio::write_all(socket, b"Hello World!\n")
    }).for_each(|(socket, buffer)| {
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

