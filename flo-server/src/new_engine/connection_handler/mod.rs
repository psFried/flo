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
use new_engine::event_stream::partition::{PartitionReader,
                                          EventFilter,
                                          PartitionSendError,
                                          Operation,
                                          ProduceResponseReceiver,
                                          ConsumeResponseReceiver,
                                          AsyncConsumeResult,
                                          ConsumerNotifier};
use event::FloEventId;
use self::consumer::consumer_stream::{Consumer,
                     ConsumerTaskSetter,
                     ConsumerStatusChecker,
                     ConsumerStatusSetter,
                     PendingConsumer,
                     prepare_consumer_start,
                     create_status_channel};

const DEFAULT_CONSUME_BATCH_SIZE: u32 = 10_000;


pub struct ConnectionHandler {
    reactor: Handle,
    client_name: Option<String>,
    connection_id: ConnectionId,
    consume_batch_size: u32,
    client_sender: ClientSender,
    engine: EngineRef,
    event_stream: EventStreamRef,
    produce_operation: Option<(u32, ProduceResponseReceiver)>,
    pending_consume_operation: Option<(PendingConsumer, ConsumeResponseReceiver)>,
    consumer_ref: Option<ConsumerStatusSetter>,
}


pub type ConnectionHandlerResult = Result<(), String>;

impl ConnectionHandler {
    pub fn new(connection: ConnectionId, client_sender: ClientSender, engine: EngineRef, handle: Handle) -> ConnectionHandler {
        let event_stream = engine.get_default_stream();
        ConnectionHandler {
            reactor: handle,
            client_name: None,
            consume_batch_size: DEFAULT_CONSUME_BATCH_SIZE,
            connection_id: connection,
            client_sender: client_sender,
            engine: engine,
            event_stream: event_stream,
            produce_operation: None,
            pending_consume_operation: None,
            consumer_ref: None,
        }
    }

    pub fn can_process(&self, message: &ProtocolMessage) -> bool {
        self.produce_operation.is_none() && self.pending_consume_operation.is_none()
    }

    pub fn handle_incoming_message(&mut self, message: ProtocolMessage) -> ConnectionHandlerResult {

        trace!("client: '{:?}', connection_id: {}, received message: {:?}", self.client_name, self.connection_id, message);

        match message {
            ProtocolMessage::SetEventStream(SetEventStream{op_id, name}) => self.set_event_stream(op_id, name),
            ProtocolMessage::Announce(announce) => self.handle_announce(announce),
            ProtocolMessage::ProduceEvent(produce) => self.handle_produce(produce),
            ProtocolMessage::NewStartConsuming(consumer_start) => self.handle_start_consuming(consumer_start),
            _ => unimplemented!()
        }
    }

    fn handle_start_consuming(&mut self, start: NewConsumerStart) -> ConnectionHandlerResult {
        let NewConsumerStart {op_id, version_vector, namespace, max_events} = start;

        match EventFilter::parse(&namespace) {
            Ok(filter) => {
                let start = version_vector.first().map(|id| id.event_counter).unwrap_or(0);
                let (pending, notifier) = prepare_consumer_start(op_id, Some(max_events));

                let result = self.event_stream.get_partition(1).unwrap().consume(
                    self.connection_id,
                    op_id,
                    notifier,
                    filter,
                    start);

                self.handle_consume_send_result(op_id, result, pending)
            }
            Err(description) => {
                self.send_to_client(ProtocolMessage::Error(ErrorMessage {
                    op_id: op_id,
                    kind: ErrorKind::InvalidNamespaceGlob,
                    description: description,
                }))
            }
        }
    }


    fn handle_consume_send_result(&mut self, op_id: u32, result: AsyncConsumeResult, pending: PendingConsumer) -> ConnectionHandlerResult {
        match result {
            Ok(recv) => {
                self.pending_consume_operation = Some((pending, recv));
                Ok(())
            }
            Err(send_err) => {
                Err(format!("Failed to send operation: {:?}", send_err.0))
            }
        }
    }

    fn handle_produce(&mut self, produce: ProduceEvent) -> ConnectionHandlerResult {
        debug_assert!(self.produce_operation.is_none());
        let op_id = produce.op_id;

        let partition = self.event_stream.get_partition(1).unwrap();
        let receiver = partition.produce(self.connection_id, op_id, vec![produce]).map_err(|err| {
            format!("Failed to send operation: {:?}", err.0)
        })?;

        self.produce_operation = Some((op_id, receiver));

        Ok(())
    }

    fn handle_announce(&mut self, announce: ClientAnnounce) -> ConnectionHandlerResult {
        let ClientAnnounce {op_id, client_name, protocol_version} = announce;
        // todo: return error if client name is already set or if protocol version != 1
        self.client_name = Some(client_name);

        let status = create_stream_status(op_id, &self.event_stream);
        self.send_to_client(ProtocolMessage::StreamStatus(status)).map_err(|err| {
            format!("Error sending message to client: {:?}", err)
        })
    }

    fn set_event_stream(&mut self, op_id: u32, name: String) -> ConnectionHandlerResult {
        use new_engine::ConnectError;
        trace!("attempting to set event stream for {:?} to '{}'", self, name);
        match self.engine.get_stream(&name) {
            Ok(new_stream) => {
                debug!("Setting event stream to '{}' for {:?}", new_stream.name(), self);
                let stream_status = create_stream_status(op_id, &new_stream);
                self.event_stream = new_stream;
                self.send_to_client(ProtocolMessage::StreamStatus(stream_status))
            }
            Err(ConnectError::NoStream) => {
                let err_message = ErrorMessage {
                    op_id: op_id,
                    kind: ErrorKind::NoSuchStream,
                    description: format!("Event stream: '{}' does not exist", name),
                };
                self.send_to_client(ProtocolMessage::Error(err_message))
            }
            Err(ConnectError::InitFailed(io_err)) => {
                let err_message = ErrorMessage {
                    op_id: op_id,
                    kind: ErrorKind::StorageEngineError,
                    description: format!("Failed to create stream: '{}': {:?}", name, io_err)
                };
                self.send_to_client(ProtocolMessage::Error(err_message))
            }
        }
    }

    fn poll_consume_complete(&mut self) -> Poll<(), io::Error> {
        let event_reader = {
            if let Some((ref pending, ref mut recv)) = self.pending_consume_operation {
                try_ready!(recv.poll().map_err(|recv_err| {
                    error!("Failed to poll consume operation for client: op_id: {}: {:?}", pending.op_id, recv_err);
                    io::Error::new(io::ErrorKind::Other, "failed to poll consume operation")
                }))
            } else {
                unreachable!() // since we've already check to make sure consume_operation is some before calling this
            }
        };

        let (pending, _) = self.pending_consume_operation.take().unwrap();

        self.spawn_consumer(event_reader, pending)
    }

    fn spawn_consumer(&mut self, reader: PartitionReader, pending: PendingConsumer) -> Poll<(), io::Error> {
        let op_id = pending.op_id;
        let send_result = self.send_to_client(ProtocolMessage::CursorCreated(CursorInfo {
            op_id: op_id,
            batch_size: self.consume_batch_size,
        }));

        if let Err(desc) = send_result {
            return Err(io::Error::new(io::ErrorKind::Other, desc));
        }

        let (status_setter, status_checker) = create_status_channel();

        let connection_id = self.connection_id;
        let consumer = Consumer::new(connection_id, self.consume_batch_size, status_checker, reader, pending);
        let future = consumer.forward(self.client_sender.clone()).map_err(move |err| {
            error!("Consumer failed for connection_id: {}, op_id: {}, err: {:?}", connection_id, op_id, err);
            ()
        }).map(move |_| {
            debug!("Successfully finished consumer for connection_id: {}, op_id: {}", connection_id, op_id);
            ()
        });

        self.consumer_ref = Some(status_setter);
        self.reactor.spawn(future);

        Ok(Async::Ready(()))
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

        self.send_to_client(response).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, e)
        })?;
        self.produce_operation = None;

        Ok(Async::Ready(()))
    }

    fn send_to_client(&self, message: ProtocolMessage) -> ConnectionHandlerResult {
        ClientSender::send(&self.client_sender, message).map_err(|e| {
            format!("Error sending outgoing message for connection_id: {}, message: {:?}", self.connection_id, e.into_inner())
        })
    }
}

fn create_stream_status(op_id: u32, stream_ref: &EventStreamRef) -> EventStreamStatus {
    let mut partition_statuses = Vec::with_capacity(stream_ref.get_partition_count() as usize);

    for partition in stream_ref.partitions() {
        let num = partition.partition_num();
        let head = partition.get_highest_event_counter();
        let primary = partition.is_primary();
        let part_status = PartitionStatus {
            partition_num: num,
            head: head,
            primary: primary,
        };
        partition_statuses.push(part_status);
    }

    EventStreamStatus {
        op_id: op_id,
        name: stream_ref.name().to_owned(),
        partitions: partition_statuses,
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
        } else if self.pending_consume_operation.is_some() {
            self.poll_consume_complete()
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
        write!(f, "ConnectionHandler{{connection_id: {}, client_sender: {:p}, engine_ref: {:?}, event_stream: {:?} }}",
               self.connection_id,
            &self.client_sender,
            self.engine,
            self.event_stream)
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

        assert_eq!(SYSTEM_STREAM_NAME, subject.event_stream.name());

        let set_stream = SetEventStream {
            op_id: 435,
            name: new_stream_name.clone()
        };
        subject.handle_incoming_message(ProtocolMessage::SetEventStream(set_stream)).expect("failed to handle message");
        assert_eq!(&new_stream_name, subject.event_stream.name());

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
        assert_eq!(SYSTEM_STREAM_NAME, subject.event_stream.name());

        let expected = ErrorMessage {
            op_id: 657,
            kind: ErrorKind::NoSuchStream,
            description: "Event stream: 'foo' does not exist".to_owned()
        };

        fixture.assert_sent_to_client(ProtocolMessage::Error(expected));
    }
}

