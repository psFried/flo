mod client_message_stream;
mod server_message_stream;


use std::net::SocketAddr;
use std::io;
#[allow(deprecated)]
use tokio_core::io::Io;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use futures::{Stream, Sink, Future};

use engine::{create_client_channels, ConnectionHandler};
use engine::connection_handler::{ConnectionHandlerInput, ConnectionControlReceiver};
use engine::{EngineRef, ConnectionId};

pub use self::client_message_stream::ProtocolMessageStream;
pub use self::server_message_stream::ServerMessageStream;



pub fn create_connection_handler(client_handle: Handle,
                                 client_engine_ref: EngineRef,
                                 connection_id: ConnectionId,
                                 client_addr: SocketAddr,
                                 tcp_stream: TcpStream,
                                 control_receiver: ConnectionControlReceiver) -> Box<Future<Item=(), Error=()>>  {

    info!("Opened connection_id: {} to address: {}", connection_id, client_addr);
    let (client_tx, client_rx) = create_client_channels();

    #[allow(deprecated)]
    let (tcp_reader, tcp_writer) = tcp_stream.split();

    let server_to_client = ServerMessageStream::new(connection_id, client_rx, tcp_writer);

    let client_message_stream = ProtocolMessageStream::new(connection_id, tcp_reader)
            .map(|proto_message| proto_message.into());

    let joint_stream = control_receiver
            .map(|control| control.into())
            .map_err(|recv_err| {
                io::Error::new(io::ErrorKind::Other, format!("Error receiving from control channel: {:?}", recv_err))
            })
            .select(client_message_stream);

    let mut system_stream_ref = client_engine_ref.get_system_stream();
    let connection_handler = ConnectionHandler::new(
        connection_id,
        client_tx.clone(),
        client_engine_ref,
        client_handle);

    let client_to_server = connection_handler
            .send_all(joint_stream)
            .map(|_| ());

    let future = client_to_server.select(server_to_client).then(move |res| {
        if let Err((err, _)) = res {
            warn!("Closing connection: {} due to err: {:?}", connection_id, err);
        }
        info!("Closed connection_id: {} to address: {}", connection_id, client_addr);
        system_stream_ref.connection_closed(connection_id);
        Ok(())
    });
    Box::new(future)
}
