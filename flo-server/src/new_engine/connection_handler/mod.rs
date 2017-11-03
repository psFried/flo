pub mod connection_state;
mod consumer;

use std::fmt::{self, Debug};
use std::io;
use std::error::Error;

use futures::sync::oneshot;
use futures::{Async, Poll, AsyncSink, StartSend, Sink, Stream, Future};
use tokio_core::reactor::Handle;

use channels::Sender;
use protocol::*;
use new_engine::{ConnectionId, ClientSender, EngineRef, SYSTEM_STREAM_NAME, system_stream_name};
use new_engine::event_stream::EventStreamRef;
use new_engine::event_stream::partition::ProduceResponseReceiver;
use event::FloEventId;
use self::connection_state::ConnectionState;
use self::consumer::ConsumerConnectionState;



pub struct ConnectionHandler {
    common_state: ConnectionState,
    consumer_state: ConsumerConnectionState,
    produce_operation: Option<(u32, ProduceResponseReceiver)>,
}


pub type ConnectionHandlerResult = Result<(), String>;

impl ConnectionHandler {
    pub fn new(connection: ConnectionId, client_sender: ClientSender, engine: EngineRef, handle: Handle) -> ConnectionHandler {
        ConnectionHandler {
            common_state: ConnectionState::new(connection, client_sender, engine, handle),
            consumer_state: ConsumerConnectionState::new(),
            produce_operation: None,
        }
    }

    pub fn can_process(&self, _message: &ProtocolMessage) -> bool {
        self.produce_operation.is_none() && !self.consumer_state.requires_poll_complete()
    }

    pub fn handle_incoming_message(&mut self, message: ProtocolMessage) -> ConnectionHandlerResult {
        trace!("client: {:?}, received message: {:?}", self.common_state, message);

        match message {
            ProtocolMessage::SetEventStream(SetEventStream{op_id, name}) => self.common_state.set_event_stream(op_id, name),
            ProtocolMessage::Announce(announce) => self.common_state.handle_announce_message(announce),
            ProtocolMessage::ProduceEvent(produce) => self.handle_produce(produce),
            ProtocolMessage::NewStartConsuming(consumer_start) => self.handle_start_consuming(consumer_start),
            _ => unimplemented!()
        }
    }

    fn handle_start_consuming(&mut self, message: NewConsumerStart) -> ConnectionHandlerResult {
        let ConnectionHandler {ref mut common_state, ref mut consumer_state, ..} = *self;
        consumer_state.handle_start_consuming(message, common_state)
    }

    fn handle_produce(&mut self, produce: ProduceEvent) -> ConnectionHandlerResult {
        debug_assert!(self.produce_operation.is_none());
        let op_id = produce.op_id;
        let connection_id = self.common_state.connection_id;

        let receiver = {
            let partition = self.common_state.event_stream.get_partition(1).unwrap();
            partition.produce(connection_id, op_id, vec![produce]).map_err(|err| {
                format!("Failed to send operation: {:?}", err.0)
            })?
        };

        self.produce_operation = Some((op_id, receiver));

        Ok(())
    }


    fn poll_produce_complete(&mut self) -> Poll<(), io::Error> {
        let response = match self.produce_operation {
            Some((op_id, ref mut pending)) => {
                let result = try_ready!(pending.poll().map_err(|recv_err| {
                    error!("Failed to poll produce operation for client: op_id: {}: {:?}", op_id, recv_err);
                    io::Error::new(io::ErrorKind::Other, "failed to poll produce operation")
                }));

                match result {
                    Ok(id) => {
                        ProtocolMessage::AckEvent(EventAck{
                            op_id: op_id,
                            event_id: id,
                        })
                    }
                    Err(io_err) => {
                        ProtocolMessage::Error(ErrorMessage {
                            op_id: op_id,
                            kind: ErrorKind::StorageEngineError,
                            description: format!("Persistence Error: {}", io_err.description()),
                        })
                    }
                }
            },
            None => return Ok(Async::Ready(()))
        };

        self.common_state.send_to_client(response).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, e)
        })?;
        self.produce_operation = None;

        Ok(Async::Ready(()))
    }

}



impl Sink for ConnectionHandler {
    type SinkItem = ProtocolMessage;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if !self.can_process(&item) {
            return Ok(AsyncSink::NotReady(item));
        }

        self.handle_incoming_message(item).map(|()| {
            AsyncSink::Ready
        }).map_err(|err_string| {
            io::Error::new(io::ErrorKind::Other, err_string)
        })
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if self.produce_operation.is_some() {
            self.poll_produce_complete()
        } else if self.consumer_state.requires_poll_complete() {
            let ConnectionHandler {ref mut common_state, ref mut consumer_state, ..} = *self;
            consumer_state.poll_consume_complete(common_state)
        } else {
            Ok(Async::Ready(()))
        }
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.poll_complete()
    }
}


impl Debug for ConnectionHandler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ConnectionHandler")
                .field("common_state", &self.common_state)
                .field("consumer_state", &self.consumer_state)
                .field("pending_produce_op", &self.produce_operation)
                .finish()
    }
}



#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use tokio_core::reactor::Core;

    use super::*;
    use protocol::*;
    use event::ActorId;
    use new_engine::event_stream::partition::*;
    use new_engine::{ClientSender, ClientReceiver, create_client_channels};
    use channels::{Sender, MockSender};
    use atomics::{AtomicCounterWriter, AtomicCounterReader, AtomicBoolReader, AtomicBoolWriter};

    struct Fixture {
        partition_receivers: HashMap<(String, ActorId), PartitionReceiver>,
        client_receiver: Option<ClientReceiver>,
        engine: EngineRef,
        reactor: Core,
    }

    impl Fixture {
        fn create() -> (ConnectionHandler, Fixture) {
            let reactor = Core::new().unwrap();

            let (client_sender, client_rx) = ::futures::sync::mpsc::unbounded();
            let counter_writer = AtomicCounterWriter::zero();
            let primary = AtomicBoolWriter::with_value(true);

            let (tx, rx) = create_partition_channels();
            let part_ref = PartitionRef::new(system_stream_name(),
                                             1,
                                             counter_writer.reader(),
                                             primary.reader(),
                                             tx);
            let stream = EventStreamRef::new(system_stream_name(), vec![part_ref]);
            let mut streams = HashMap::new();
            streams.insert(system_stream_name(), stream);
            let engine = EngineRef::new(streams);

            let subject = ConnectionHandler::new(456, client_sender, engine.clone(), reactor.handle());

            let mut partition_receivers = HashMap::new();
            partition_receivers.insert((system_stream_name(), 1), rx);

            let fixture = Fixture {
                partition_receivers: partition_receivers,
                client_receiver: Some(client_rx),
                engine: engine,
                reactor: reactor
            };
            (subject, fixture)
        }

        fn with_stream(stream_name: &str, partition_count: ActorId) -> (ConnectionHandler, Fixture) {
            let (handler, mut fixutre) = Fixture::create();
            fixutre.add_new_stream(stream_name, partition_count);
            (handler, fixutre)
        }

        fn add_new_stream(&mut self, name: &str, num_partitions: ActorId) {
            let mut partition_refs = Vec::with_capacity(num_partitions as usize);
            for i in 0..num_partitions {
                let partition_num = i + 1;
                let counter_writer = AtomicCounterWriter::zero();
                let primary = AtomicBoolWriter::with_value(true);
                let (tx, rx) = create_partition_channels();
                let part_ref = PartitionRef::new(name.to_owned(),
                                                 partition_num,
                                                 counter_writer.reader(),
                                                 primary.reader(),
                                                 tx);
                partition_refs.push(part_ref);
            }
            partition_refs.sort_by_key(|p| p.partition_num());
            let stream_ref = EventStreamRef::new(name.to_owned(), partition_refs);
            self.engine.event_streams.lock().map(|mut map| {
                map.insert(name.to_owned(), stream_ref)
            }).unwrap();
        }

        fn message_sent_to_partition(&self, event_stream: &str, partition_id: ActorId) -> Operation {
            let key = (event_stream.to_owned(), partition_id);
            let partition_receiver = self.partition_receivers.get(&key).expect("no such partition");
            let timeout = ::std::time::Duration::new(0, 0);
            let result = partition_receiver.recv_timeout(timeout);
            result.expect(&format!("partition: {} failed to receive message", partition_id))
        }

        fn assert_sent_to_client(&mut self, expected: ProtocolMessage) {
            use tokio_core::reactor::Timeout;
            use futures::future::Either;

            let recv = self.client_receiver.take().unwrap();
            let timeout = Timeout::new(::std::time::Duration::from_millis(100), &self.reactor.handle());
            let future = recv.into_future().select2(timeout);

            let result = self.reactor.run(future);
            match result {
                Ok(Either::A(((message, receiver), _))) => {
                    self.client_receiver = Some(receiver);
                    assert_eq!(Some(expected), message)
                },
                Ok(Either::B(_)) => panic!("Timed out on recv with Ok"),
                Err(Either::A(_)) => panic!("Recv err attempting to recv next message, expected: {:?}", expected),
                Err(Either::B(_)) => panic!("Timout Err")
            }

        }

    }

    #[test]
    fn set_event_stream_sets_event_stream_when_the_named_stream_exists() {
        let (mut subject, mut fixture) = Fixture::create();

        let new_stream_name = "foo".to_owned();
        fixture.add_new_stream(&new_stream_name, 3);

        assert_eq!(SYSTEM_STREAM_NAME, subject.common_state.event_stream.name());

        let set_stream = SetEventStream {
            op_id: 435,
            name: new_stream_name.clone()
        };
        subject.handle_incoming_message(ProtocolMessage::SetEventStream(set_stream)).expect("failed to handle message");
        assert_eq!(&new_stream_name, subject.common_state.event_stream.name());

        let expected = EventStreamStatus {
            op_id: 435,
            name: new_stream_name,
            partitions: vec![
                PartitionStatus {
                    partition_num: 1,
                    head: 0,
                    primary: true,
                },
                PartitionStatus {
                    partition_num: 2,
                    head: 0,
                    primary: true,
                },
                PartitionStatus {
                    partition_num: 3,
                    head: 0,
                    primary: true,
                },
            ],
        };

        fixture.assert_sent_to_client(ProtocolMessage::StreamStatus(expected));
    }

    #[test]
    fn set_event_stream_sends_error_message_when_named_stream_does_not_exist() {
        let (mut subject, mut fixture) = Fixture::create();

        let set_stream = SetEventStream {
            op_id: 657,
            name: "foo".to_owned()
        };
        subject.handle_incoming_message(ProtocolMessage::SetEventStream(set_stream)).expect("failed to handle message");
        assert_eq!(SYSTEM_STREAM_NAME, subject.common_state.event_stream.name());

        let expected = ErrorMessage {
            op_id: 657,
            kind: ErrorKind::NoSuchStream,
            description: "Event stream: 'foo' does not exist".to_owned()
        };

        fixture.assert_sent_to_client(ProtocolMessage::Error(expected));
    }
}

