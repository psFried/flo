mod client_message_stream;
mod server_message_stream;

use server::engine::api::{next_connection_id, ServerMessage};
use futures::sync::mpsc::UnboundedReceiver;
use protocol::ClientProtocolImpl;
use tokio_core::net::TcpStream;
use tokio_core::io::{copy, ReadHalf, WriteHalf, Io};

use std::sync::mpsc as std_mpsc;

pub use self::client_message_stream::ClientMessageStream;



pub fn accept_connection(tcp_stream: TcpStream, server_receiver: UnboundedReceiver<ServerMessage>) {
    let connection_id = next_connection_id();

    let (tcp_reader, tcp_writer) = tcp_stream.split();

    let client_to_server = ClientMessageStream::new(connection_id, tcp_reader, ClientProtocolImpl);

    //TODO: handle messages from server to client
}



