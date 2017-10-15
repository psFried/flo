//! For running an event stream server in-process and using an in-memory transport for communication with it.
//! This is especially useful in development and testing, as it allows an application to run without a dependency
//! on an external server.

use std::fmt::Debug;
use std::io;

use tokio_core::reactor::{Handle, Remote};
use futures::Stream;

use flo_client_lib::async::{AsyncClient, MessageReceiver, MessageSender};
use flo_client_lib::codec::EventCodec;

use new_engine::{EngineRef, ClientSender, ClientReceiver, create_client_channels, start_controller, ConnectionHandler};

pub use new_engine::ControllerOptions;
pub use new_engine::event_stream::EventStreamOptions;


#[derive(Clone, Debug)]
pub struct EmbeddedFloServer {
    engine_ref: EngineRef,
}

impl EmbeddedFloServer {

    pub fn connect_client<D: Debug>(&self, name: String, codec: Box<EventCodec<EventData=D>>, handle: Handle) -> AsyncClient<D> {
        let engine_ref = self.engine_ref.clone();
        let connection_id = engine_ref.next_connection_id();
        let (client_sender, client_receiver) = create_client_channels();

        let connection_handler = ConnectionHandler::new(connection_id,
                                                            client_sender.clone(),
                                                            engine_ref,
                                                             handle);

        let receiver = client_receiver.map_err(|recv_err| {
            io::Error::new(io::ErrorKind::UnexpectedEof, format!("Error reading from channel: {:?}", recv_err))
        });
        let recv = Box::new(receiver) as MessageReceiver;
        let send = Box::new(connection_handler) as MessageSender;

        AsyncClient::new(name, send, recv, codec)
    }
}


pub fn run_embedded_server(options: ControllerOptions) -> io::Result<EmbeddedFloServer> {
    start_controller(options).map(|engine_ref| {
        EmbeddedFloServer {
            engine_ref: engine_ref,
        }
    })
}

