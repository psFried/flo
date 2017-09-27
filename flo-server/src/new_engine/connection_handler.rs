
use futures::sync::oneshot;
use futures::{Async, Poll, AsyncSink, StartSend, Sink, Future};

use protocol::{ProtocolMessage, ConsumerStart};
use new_engine::{ConnectionId, ClientSender, EngineRef, DEFAULT_STREAM_NAME, default_stream_name};
use new_engine::event_stream::EventStreamRef;
use new_engine::event_stream::partition::PartitionReader;


pub type ConsumerStartSender = oneshot::Sender<PartitionReader>;
pub type ConsumerStartReceiver = oneshot::Receiver<PartitionReader>;

pub fn create_consumer_start_oneshot() -> (ConsumerStartSender, ConsumerStartReceiver) {
    oneshot::channel()
}



struct ConnectionHandler {
    connection_id: ConnectionId,
    client_sender: ClientSender,
    engine: EngineRef,
    event_stream: EventStreamRef,
}


impl ConnectionHandler {
    pub fn new(connection: ConnectionId, client_sender: ClientSender, engine: EngineRef) -> ConnectionHandler {
        let event_stream = engine.get_default_stream();
        ConnectionHandler {
            connection_id: connection,
            client_sender: client_sender,
            engine: engine,
            event_stream: event_stream,
        }
    }

    pub fn handle_incoming_message(&mut self, message: ProtocolMessage) -> Result<(), String> {
        match message {
            _ => unimplemented!()
        }
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


#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use protocol::*;
    use event::ActorId;
    use new_engine::event_stream::partition::*;
    use new_engine::{ClientSender, ClientReceiver, create_client_channels};


    struct Fixture {
        partition_receivers: HashMap<(String, ActorId), PartitionReceiver>,
        client_receiver: ClientReceiver,
        engine: EngineRef,
    }

    impl Fixture {
        fn create() -> (ConnectionHandler, Fixture) {
            let (client_sender, client_receiver) = create_client_channels();

            let (tx, rx) = create_partition_channels();
            let part_ref = PartitionRef::new(default_stream_name(), 1, tx);
            let stream = EventStreamRef::new(default_stream_name(), vec![part_ref]);
            let mut streams = HashMap::new();
            streams.insert(default_stream_name(), stream);
            let engine = EngineRef::new(streams);

            let subject = ConnectionHandler::new(456, client_sender, engine.clone());

            let mut partition_receivers = HashMap::new();
            partition_receivers.insert((default_stream_name(), 1), rx);

            let fixture = Fixture {
                partition_receivers: partition_receivers,
                client_receiver: client_receiver,
                engine: engine,
            };
            (subject, fixture)
        }

        fn add_new_stream(&mut self, name: &str, num_partitions: ActorId) {
            let mut partition_refs = Vec::with_capacity(num_partitions as usize);
            for i in 0..num_partitions {
                let partition_num = i + 1;
                let (tx, rx) = create_partition_channels();
                let part_ref = PartitionRef::new(name.to_owned(), partition_num, tx);
                partition_refs.push(part_ref);
            }
            partition_refs.sort_by_key(|p| p.partition_num());
            let stream_ref = EventStreamRef::new(name.to_owned(), partition_refs);
            self.engine.event_streams.lock().map(|mut map| {
                map.insert(name.to_owned(), stream_ref)
            }).unwrap();
        }

    }

    #[test]
    fn set_event_stream() {
        let (mut subject, mut fixture) = Fixture::create();

        assert_eq!(DEFAULT_STREAM_NAME, subject.event_stream.name());
    }
}

