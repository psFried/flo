use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;

use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
#[allow(deprecated)]
use tokio_core::io::Io;
use futures::{Future, Stream, Sink};

use event::VersionVector;
use event_loops::LoopHandles;
use engine::controller::SystemStreamRef;
use engine::{EngineRef, create_client_channels, ReceivedProtocolMessage, ClientSender, ClientReceiver};
use engine::system_stream::{FloInstanceId, AppendEntriesCall, RequestVoteCall};

use super::ConnectionSendResult;


/// Trait representing an active peer connection, with functions to control it
pub trait PeerSystemConnection: Debug + Send + 'static {
    fn send_append_entries(&mut self, append_entries: AppendEntriesCall) -> ConnectionSendResult<AppendEntriesCall>;
    fn send_request_vote(&mut self, request: RequestVoteCall) -> ConnectionSendResult<RequestVoteCall>;
}



enum SystemConnectionMessage {
    AppendEntries(AppendEntriesCall),
    Vote(RequestVoteCall),
}

fn create_outgoing_connection(loops: &mut LoopHandles, client_addr: SocketAddr, engine_ref: EngineRef) -> Box<PeerSystemConnection> {

    // These copies are to work around the ownership rules, since we want to have slightly different error handling for
    // connection failures depending on when it fails
    let client_addr_copy = client_addr.clone();

    let mut system_stream = engine_ref.get_system_stream();
    let mut system_stream_copy = system_stream.clone();

    let (client_tx, client_rx) = create_client_channels();
    let client_tx_copy = client_tx.clone();

    loops.next_handle().spawn( move |handle| {

        let owned_handle = handle.clone();
        let addr = client_addr_copy;
        TcpStream::connect(&addr, handle).map_err( move |io_err| {

            error!("Failed to create outgoing connection to address: {:?}: {:?}", addr, io_err);
            system_stream.outgoing_connection_failed(0, addr);

        }).and_then( move |tcp_stream| {

            outgoing_connection_future(owned_handle,
                                       engine_ref,
                                       client_addr,
                                       tcp_stream,
                                       client_tx_copy,
                                       client_rx,
                                       system_stream_copy)
        })
    });

    let outgoing = OutgoingPeerSystemConnection {
        client_tx
    };
    Box::new(outgoing)
}

fn outgoing_connection_future(handle: Handle,
                              engine_ref: EngineRef,
                              client_addr: SocketAddr,
                              tcp_stream: TcpStream,
                              client_tx: ClientSender,
                              client_rx: ClientReceiver,
                              mut system_stream: SystemStreamRef) -> Box<Future<Item=(), Error=()>> {

    use flo_io::{ProtocolMessageStream, ServerMessageStream};
    use engine::ConnectionHandler;

    let connection_id = engine_ref.next_connection_id();
    debug!("Established connection to {:?} with connection_id: {}", client_addr, connection_id);
    if let Err(io_err) = tcp_stream.set_nodelay(true) {
        error!("Error setting NODELAY for connection_id: {}. Nagle yet lives!: {:?}", connection_id, io_err);
    }
    #[allow(deprecated)]
    let (read, write) = tcp_stream.split();
    let server_to_client = ServerMessageStream::new(connection_id, client_rx, write);
    let client_message_stream = ProtocolMessageStream::new(connection_id, read);
    let mut connection_handler = ConnectionHandler::new(
        connection_id,
        client_tx,
        engine_ref,
        handle.clone());

    // sends PeerAnnounce message to the other server and sets up the connection handler to expect the response
    connection_handler.upgrade_to_outgoing_peer();

    let client_to_server = connection_handler
            .send_all(client_message_stream)
            .map(|_| ());

    let future = client_to_server.select(server_to_client).then(move |res| {
        if let Err((err, _)) = res {
            warn!("Closing outgoing connection: {} due to err: {:?}", connection_id, err);
            system_stream.outgoing_connection_failed(connection_id, client_addr);
        }
        info!("Closed connection_id: {} to address: {}", connection_id, client_addr);
        Ok(())
    });
    Box::new(future)
}

#[derive(Debug)]
struct OutgoingPeerSystemConnection {
    client_tx: ClientSender
}

impl PeerSystemConnection for OutgoingPeerSystemConnection {
    fn send_append_entries(&mut self, append_entries: AppendEntriesCall) -> ConnectionSendResult<AppendEntriesCall> {
        unimplemented!()
    }

    fn send_request_vote(&mut self, request: RequestVoteCall) -> ConnectionSendResult<RequestVoteCall> {
        unimplemented!()
    }
}
