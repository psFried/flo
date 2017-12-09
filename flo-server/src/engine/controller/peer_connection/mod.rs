mod replication;
mod system;

use std::fmt::Debug;
use std::net::SocketAddr;
use std::io;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use futures::Future;

use engine::EngineRef;
use engine::connection_handler::{create_connection_control_channels, ConnectionControlSender};
use event_loops::LoopHandles;
use flo_io::create_connection_handler;

pub use self::replication::PeerReplicationConnection;
pub use self::system::PeerSystemConnection;

pub type ConnectionSendResult<T> = Result<(), T>;


/// Trait for creating outgoing connections (clever name, I know).
pub trait OutgoingConnectionCreator {
    fn establish_system_connection(&mut self, address: SocketAddr) -> Box<PeerSystemConnection>;
    fn establish_replication_connection(&mut self, address: SocketAddr, event_stream: String) -> Box<PeerReplicationConnection>;
}

pub struct OutgoingConnectionCreatorImpl {
    event_loops: LoopHandles,
    engine_ref: EngineRef,
}

impl OutgoingConnectionCreator for OutgoingConnectionCreatorImpl {
    fn establish_system_connection(&mut self, address: SocketAddr) -> Box<PeerSystemConnection> {
        unimplemented!()
    }

    fn establish_replication_connection(&mut self, address: SocketAddr, event_stream: String) -> Box<PeerReplicationConnection> {
        unimplemented!()
    }
}


fn create_outgoing_connection(loops: &mut LoopHandles, client_addr: SocketAddr, engine_ref: EngineRef) -> ConnectionControlSender {

    let client_addr_copy = client_addr.clone();
    let mut system_stream = engine_ref.get_system_stream();

    let (control_tx, control_rx) = create_connection_control_channels();

    loops.next_handle().spawn( move |handle| {

        let owned_handle = handle.clone();
        let addr = client_addr_copy;
        TcpStream::connect(&addr, handle).map_err( move |io_err| {

            error!("Failed to create outgoing connection to address: {:?}: {:?}", addr, io_err);
            system_stream.outgoing_connection_failed(0, addr);

        }).and_then( move |tcp_stream| {
            let connection_id = engine_ref.next_connection_id();

            create_connection_handler(owned_handle,
                                      engine_ref,
                                      connection_id,
                                      client_addr,
                                      tcp_stream,
                                      control_rx)
        })
    });

    control_tx
}



