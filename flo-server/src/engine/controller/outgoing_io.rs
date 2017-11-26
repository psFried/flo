use std::net::SocketAddr;
use std::io;

use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;

#[allow(deprecated)]
use tokio_core::io::Io;
use futures::{Future, Stream, Sink};

use event_loops::LoopHandles;
use engine::{EngineRef, create_client_channels, ReceivedProtocolMessage};

pub struct CreateOutgoingConnection(SocketAddr);

impl CreateOutgoingConnection {
    pub fn new(addr: SocketAddr) -> CreateOutgoingConnection {
        CreateOutgoingConnection(addr)
    }

    fn addr(&self) -> &SocketAddr {
        &self.0
    }
}

pub struct OutgoingConnectionCreator(::futures::sync::mpsc::UnboundedSender<CreateOutgoingConnection>);


pub fn start_outgoing_io(mut event_loops: LoopHandles, engine_ref: EngineRef) -> OutgoingConnectionCreator {
    let (tx, rx) = ::futures::sync::mpsc::unbounded::<CreateOutgoingConnection>();

    let remote = event_loops.next_handle();
    let system_stream_ref = engine_ref.get_system_stream();

    let future = rx.for_each( move |create_outgoing| {

        let engine = engine_ref.clone();
        event_loops.next_handle().spawn(|handle| {
            handle_outgoing_connection(handle.clone(), create_outgoing, engine)
        });
        Ok(())
    });

    remote.spawn(|handle| {
        future
    });

    OutgoingConnectionCreator(tx)
}


fn handle_outgoing_connection(handle: Handle, create_outgoing: CreateOutgoingConnection, engine_ref: EngineRef) -> Box<Future<Item=(), Error=()>> {
    use flo_io::{ProtocolMessageStream, ServerMessageStream};
    use engine::ConnectionHandler;

    // These copies are to work around the ownership rules, since we want to have slightly different error handling for
    // connection failures depending on when it fails
    let client_addr = *create_outgoing.addr();
    let client_addr_copy = client_addr.clone();

    let mut system_stream = engine_ref.get_system_stream();
    let mut system_stream_copy = system_stream.clone();

    let future = TcpStream::connect(create_outgoing.addr(), &handle).map_err( move |io_err| {

        error!("Failed to create outgoing connection to address: {:?}: {:?}", client_addr_copy, io_err);
        system_stream.outgoing_connection_failed(0, client_addr_copy);

    }).and_then( move |tcp_stream| {
        let connection_id = engine_ref.next_connection_id();
        debug!("Established connection to {:?} with connection_id: {}", client_addr, connection_id);

        if let Err(io_err) = tcp_stream.set_nodelay(true) {
            error!("Error setting NODELAY for connection_id: {}. Nagle yet lives!: {:?}", connection_id, io_err);
        }

        let (client_tx, client_rx) = create_client_channels();

        #[allow(deprecated)]
        let (read, write) = tcp_stream.split();

        let server_to_client = ServerMessageStream::new(connection_id, client_rx, write);
        let client_message_stream = ProtocolMessageStream::new(connection_id, read);

        let connection_handler = ConnectionHandler::new(
            connection_id,
            client_tx.clone(),
            engine_ref,
            handle.clone());

        let client_to_server = connection_handler
                .send_all(client_message_stream)
                .map(|_| ());

        client_to_server.select(server_to_client).then(move |res| {
            if let Err((err, _)) = res {
                warn!("Closing outgoing connection: {} due to err: {:?}", connection_id, err);
                system_stream_copy.outgoing_connection_failed(connection_id, client_addr);
            }
            info!("Closed connection_id: {} to address: {}", connection_id, client_addr);
            Ok(())
        })

    });

    Box::new(future)
}
