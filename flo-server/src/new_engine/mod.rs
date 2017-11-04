pub mod event_stream;

mod controller;
mod connection_handler;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize};

use protocol::ProtocolMessage;

use self::event_stream::EventStreamRef;

pub use self::controller::{ControllerOptions, start_controller};
pub use self::connection_handler::{ConnectionHandler, ConnectionHandlerResult};

pub type ConnectionId = usize;

pub type ClientSender = ::futures::sync::mpsc::UnboundedSender<ProtocolMessage>;
pub type ClientReceiver = ::futures::sync::mpsc::UnboundedReceiver<ProtocolMessage>;

pub fn create_client_channels() -> (ClientSender, ClientReceiver) {
    ::futures::sync::mpsc::unbounded()
}


pub static SYSTEM_STREAM_NAME: &'static str = "system";

pub fn system_stream_name() -> String {
    SYSTEM_STREAM_NAME.to_owned()
}

#[derive(Clone, Debug)]
pub struct EngineRef {
    current_connection_id: Arc<AtomicUsize>,
    event_streams: Arc<Mutex<HashMap<String, EventStreamRef>>>
}

#[derive(Debug)]
pub enum ConnectError {
    InitFailed(::std::io::Error),
    NoStream,
}

impl EngineRef {
    pub fn new(streams: HashMap<String, EventStreamRef>) -> EngineRef {
        if !streams.contains_key(SYSTEM_STREAM_NAME) {
            panic!("Cannot create engine ref without a default stream");
        }

        EngineRef {
            current_connection_id: Arc::new(AtomicUsize::new(0)),
            event_streams: Arc::new(Mutex::new(streams))
        }
    }

    pub fn next_connection_id(&self) -> ConnectionId {
        let old = self.current_connection_id.fetch_add(1, ::std::sync::atomic::Ordering::SeqCst);
        old + 1
    }

    pub fn get_stream(&self, stream_name: &str) -> Result<EventStreamRef, ConnectError> {
        let streams = self.event_streams.lock().unwrap();
        if let Some(stream) = streams.get(stream_name).map(|s| s.clone()) {
            Ok(stream)
        } else {
            Err(ConnectError::NoStream)
        }
    }

    pub fn get_default_stream(&self) -> EventStreamRef {
        let guard = self.event_streams.lock().unwrap();
        guard.get(SYSTEM_STREAM_NAME).unwrap().clone()
    }
}




