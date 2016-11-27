use server::engine::api::{ConnectionId, ServerMessage};
use protocol::ServerProtocol;

use futures::stream::Stream;
use futures::sync::mpsc::UnboundedReceiver;
use tokio_core::net::TcpStream;
use tokio_core::io as nio;



pub struct ServerMessageStream<P: ServerProtocol> {
    connection_id: ConnectionId,
    buffer: Vec<u8>,
    tcp_writer: nio::WriteHalf<TcpStream>,
    server_receiver: UnboundedReceiver<ServerMessage>,
    protocol: Option<P>,
    buffer_start: usize,
    buffer_end: usize,
}
//
//impl Stream for ServerMessageStream {
//    type Item = usize;
//    type Error = String;
//
//    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
//        let ServerMessageStream {
//            ref connection_id,
//            ref mut tcp_writer,
//            ref mut server_receiver,
//            ref mut protocol,
//            ref mut buffer,
//            ref mut buffer_start,
//            ref mut buffer_end,
//        } = *self;
//
//    }
//}


