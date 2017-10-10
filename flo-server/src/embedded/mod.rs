//! For running an event stream server in-process and using an in-memory transport for communication with it.
//! This is especially useful in development and testing, as it allows an application to run without a dependency
//! on an external server.

use std::fmt::Debug;
use std::io;

use tokio_core::reactor::{Handle};

use flo_client_lib::async::{AsyncClient, MessageReceiver, MessageSender};
use flo_client_lib::codec::EventCodec;

use new_engine::{EngineRef, ControllerOptions, ClientSender, ClientReceiver, create_client_channels, start_controller, ConnectionHandlerImpl};
use event_loops::{LoopHandles, spawn_event_loop_thread};


#[derive(Clone, Debug)]
pub struct EmbeddedFloServer {
    loop_handles: LoopHandles,
    engine_ref: EngineRef,
}

impl EmbeddedFloServer {

    pub fn connect_client<D: Debug>(&self, name: String, codec: Box<EventCodec<EventData=D>>) -> AsyncClient<D> {
        let engine_ref = self.engine_ref.clone();
        let connection_id = engine_ref.next_connection_id();
        let (client_sender, client_receiver) = create_client_channels();

        let connection_handler = ConnectionHandlerImpl::new(connection_id,
                                                            client_sender.clone(),
                                                            engine_ref);

        let receiver = client_receiver.map_err(|recv_err| {
            io::Error::new(io::ErrorKind::UnexpectedEof, format!("Error reading from channel: {:?}", recv_err))
        });
        let recv = Box::new(client_receiver) as MessageReceiver;
        let send = Box::new(connection_handler) as MessageSender;

        AsyncClient::new(name, send, recv, codec)
    }
}


fn run_embedded_server(options: ControllerOptions) -> io::Result<EmbeddedFloServer> {

    let engine_ref = start_controller(options);
    unimplemented!()
}

