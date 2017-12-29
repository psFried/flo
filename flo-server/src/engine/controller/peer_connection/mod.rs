mod system;

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::io;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use futures::Future;

use engine::{EngineRef, ConnectionId};
use engine::connection_handler::{create_connection_control_channels, ConnectionControlSender};
use engine::controller::ConnectionRef;
use event_loops::LoopHandles;
use flo_io::create_connection_handler;
use self::system::PeerConnectionImpl;

pub use self::system::{PeerSystemConnection, PendingSystemConnection};

pub type ConnectionSendResult<T> = Result<(), T>;


/// Trait for creating outgoing connections (clever name, I know).
pub trait OutgoingConnectionCreator: Debug + Send + 'static {
    fn establish_system_connection(&mut self, address: SocketAddr) -> ConnectionRef;
}

#[derive(Debug)]
pub struct OutgoingConnectionCreatorImpl {
    event_loops: LoopHandles,
    engine_ref: EngineRef,
}

impl OutgoingConnectionCreatorImpl {
    pub fn new(loops: LoopHandles, engine: EngineRef) -> OutgoingConnectionCreatorImpl {
        OutgoingConnectionCreatorImpl {
            event_loops: loops,
            engine_ref: engine,
        }
    }
}

impl OutgoingConnectionCreator for OutgoingConnectionCreatorImpl {
    fn establish_system_connection(&mut self, address: SocketAddr) -> ConnectionRef {
        let OutgoingConnectionCreatorImpl { ref mut event_loops, ref engine_ref } = *self;

        let (sender, connection_id) = create_outgoing_connection(event_loops, address, engine_ref.clone());
        ConnectionRef {
            connection_id,
            remote_address: address,
            control_sender: sender,
        }
    }
}


fn create_outgoing_connection(loops: &mut LoopHandles, client_addr: SocketAddr, engine_ref: EngineRef) -> (ConnectionControlSender, ConnectionId) {
    let connection_id = engine_ref.next_connection_id();
    let client_addr_copy = client_addr.clone();
    let mut system_stream = engine_ref.get_system_stream();

    let (control_tx, control_rx) = create_connection_control_channels();

    loops.next_handle().spawn( move |handle| {

        let owned_handle = handle.clone();
        let addr = client_addr_copy;
        TcpStream::connect(&addr, handle).map_err( move |io_err| {

            error!("Failed to create outgoing connection to address: {:?}: {:?}", addr, io_err);
            system_stream.outgoing_connection_failed(connection_id, addr);

        }).and_then( move |tcp_stream| {

            create_connection_handler(owned_handle,
                                      engine_ref,
                                      connection_id,
                                      client_addr,
                                      tcp_stream,
                                      control_rx)
        })
    });

    (control_tx, connection_id)
}

#[cfg(test)]
pub use self::test::MockOutgoingConnectionCreator;

#[cfg(test)]
mod test {
    use super::*;
    use engine::connection_handler::ConnectionControlReceiver;

    #[derive(Debug)]
    pub struct MockOutgoingConnectionCreator {
        current_connection_id: ConnectionId,
        to_return: Vec<(SocketAddr, ConnectionRef)>,
        calls: Vec<SocketAddr>,
    }

    impl MockOutgoingConnectionCreator {
        pub fn new() -> MockOutgoingConnectionCreator {
            MockOutgoingConnectionCreator {
                current_connection_id: 0,
                to_return: Vec::new(),
                calls: Vec::new()
            }
        }

        pub fn stub(&mut self, expected_address: SocketAddr) -> (ConnectionRef, ConnectionControlReceiver) {
            let (tx, rx) = ::engine::connection_handler::create_connection_control_channels();

            self.current_connection_id += 1;
            let connection_ref = ConnectionRef {
                connection_id: self.current_connection_id,
                remote_address: expected_address,
                control_sender: tx,
            };
            self.to_return.push((expected_address, connection_ref.clone()));
            (connection_ref, rx)
        }

        pub fn boxed(self) -> Box<OutgoingConnectionCreator> {
            Box::new(self)
        }
    }

    impl OutgoingConnectionCreator for MockOutgoingConnectionCreator {
        fn establish_system_connection(&mut self, address: SocketAddr) -> ConnectionRef {
            let stub_idx = self.to_return.iter().position(|stub| stub.0 == address);
            if stub_idx.is_none() {
                panic!("Missing stub for address: {:?}", address);
            }
            self.calls.push(address);
            self.to_return.remove(stub_idx.unwrap()).1
        }
    }
}

