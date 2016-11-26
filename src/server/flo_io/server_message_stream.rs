use server::engine::api::ConnectionId;
use tokio_core::net::TcpStream;
use tokio_core::io as nio;



pub struct ServerMessageStream {
    connection_id: ConnectionId,
    tcp_writer: nio::WriteHalf<TcpStream>,
}
