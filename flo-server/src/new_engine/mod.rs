mod controller;
mod event_stream;
mod connection_handler;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{Stream, Sink, Future, Poll, Async, StartSend, AsyncSink, BoxFuture};
use protocol::{ProtocolMessage, ConsumerStart};

use self::event_stream::{EventStreamRef, };
use self::event_stream::partition::Operation;
use self::event_stream::partition::PartitionReader;


pub type ConnectionId = u64;

pub type ClientSender = ::futures::sync::mpsc::UnboundedSender<ProtocolMessage>;
pub type ClientReceiver = ::futures::sync::mpsc::UnboundedReceiver<ProtocolMessage>;

pub fn create_client_channels() -> (ClientSender, ClientReceiver) {
    ::futures::sync::mpsc::unbounded()
}


pub static DEFAULT_STREAM_NAME: &'static str = "default";

pub fn default_stream_name() -> String {
    DEFAULT_STREAM_NAME.to_owned()
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
        if !streams.contains_key(DEFAULT_STREAM_NAME) {
            panic!("Cannot create engine ref without a default stream");
        }

        EngineRef {
            current_connection_id: Arc::new(AtomicUsize::new(0)),
            event_streams: Arc::new(Mutex::new(streams))
        }
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
        guard.get(DEFAULT_STREAM_NAME).unwrap().clone()
    }
}




