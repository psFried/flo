mod controller;
mod event_stream;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::{Stream, Sink, Future, Poll, Async, StartSend, AsyncSink, BoxFuture};
use protocol::{ProtocolMessage, ConsumerStart};

use self::event_stream::EventStreamRef;
use self::event_stream::partition::Operation;
use self::event_stream::partition::PartitionReader;


pub type ConnectionId = u64;

pub type ClientSender = ::futures::sync::mpsc::UnboundedSender<ProtocolMessage>;
pub type ClientReceiver = ::futures::sync::mpsc::UnboundedReceiver<ProtocolMessage>;

pub fn create_client_channels() -> (ClientSender, ClientReceiver) {
    ::futures::sync::mpsc::unbounded()
}

pub type ConsumerStartSender = ::futures::sync::oneshot::Sender<PartitionReader>;
pub type ConsumerStartReceiver = ::futures::sync::oneshot::Receiver<PartitionReader>;

pub fn create_consumer_start_oneshot() -> (ConsumerStartSender, ConsumerStartReceiver) {
    ::futures::sync::oneshot::channel()
}

pub struct EngineRef {
    current_connection_id: ::std::sync::atomic::AtomicUsize,
    event_streams: Mutex<HashMap<String, EventStreamRef>>
}

#[derive(Debug)]
pub enum ConnectError {
    InitFailed(::std::io::Error),
    NoStream,
}

impl EngineRef {
    pub fn get_stream(&self, stream_name: &str) -> Result<EventStreamRef, ConnectError> {
        let streams = self.event_streams.lock().unwrap();
        if let Some(stream) = streams.get(stream_name).map(|s| s.clone()) {
            Ok(stream)
        } else {
            Err(ConnectError::NoStream)
        }
    }

    pub fn get_default_stream(&self) -> EventStreamRef {
        unimplemented!()
    }
}


//pub fn connection_handler(client_sender: ClientSender, client_receiver: ClientReceiver, engine: EngineRef) -> BoxFuture {
//
//    let default_event_stream = engine.get_default_stream();
//    let mut handler = ConnectionHandler {
//        client_sender: client_sender,
//        engine: engine,
//        event_stream: default_event_stream,
//    };
//
//    client_receiver.for_each(|client_message| {
//        handler.handle_incoming_message(client_message)
//    }).boxed()
//}


struct ConnectionHandler {
    connection_id: ConnectionId,
    client_sender: ClientSender,
    engine: EngineRef,
    event_stream: EventStreamRef,
}




impl ConnectionHandler {


    pub fn handle_incoming_message(&mut self, message: ProtocolMessage) -> Result<(), String> {
        match message {
            ProtocolMessage::StartConsuming(start) => self.start_consuming(start),
            _ => unimplemented!()
        }
    }

    fn start_consuming(&mut self, start: ConsumerStart) -> Result<(), String> {
        unimplemented!()
    }
}


impl Sink for ConnectionHandler {
    type SinkItem = ProtocolMessage;
    type SinkError = String;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.handle_incoming_message(item).map(|()| {
            AsyncSink::Ready
        })
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}
