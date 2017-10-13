
use std::fmt::{self, Debug};
use std::io;
use std::error::Error;

use futures::sync::oneshot;
use futures::{Async, Poll, AsyncSink, StartSend, Sink, Future};

use channels::Sender;
use protocol::*;
use new_engine::{ConnectionId, ClientSender, EngineRef, SYSTEM_STREAM_NAME, system_stream_name};
use new_engine::event_stream::EventStreamRef;
use new_engine::event_stream::partition::{PartitionReader, PartitionSendError, Operation};
use event::FloEventId;


pub type ConsumerStartSender = oneshot::Sender<PartitionReader>;
pub type ConsumerStartReceiver = oneshot::Receiver<PartitionReader>;

pub fn create_consumer_start_oneshot() -> (ConsumerStartSender, ConsumerStartReceiver) {
    oneshot::channel()
}

pub type ConnectionHandlerImpl = ConnectionHandler<ClientSender>;

pub struct ConnectionHandler<C: Sender<ProtocolMessage>> {
    client_name: Option<String>,
    connection_id: ConnectionId,
    client_sender: C,
    engine: EngineRef,
    event_stream: EventStreamRef,
    produce_operation: Option<(u32, oneshot::Receiver<io::Result<FloEventId>>)>,
}


pub type ConnectionHandlerResult = Result<(), String>;

impl <C: Sender<ProtocolMessage>> ConnectionHandler<C> {
    pub fn new(connection: ConnectionId, client_sender: C, engine: EngineRef) -> ConnectionHandler<C> {
        let event_stream = engine.get_default_stream();
        ConnectionHandler {
            client_name: None,
            connection_id: connection,
            client_sender: client_sender,
            engine: engine,
            event_stream: event_stream,
            produce_operation: None,
        }
    }

    pub fn can_process(&self, message: &ProtocolMessage) -> bool {
        self.produce_operation.is_none()
    }

    pub fn handle_incoming_message(&mut self, message: ProtocolMessage) -> ConnectionHandlerResult {


        trace!("client: '{:?}', connection_id: {}, received message: {:?}", self.client_name, self.connection_id, message);

        match message {
            ProtocolMessage::SetEventStream(SetEventStream{op_id, name}) => self.set_event_stream(op_id, name),
            ProtocolMessage::Announce(announce) => self.handle_announce(announce),
            ProtocolMessage::ProduceEvent(produce) => self.handle_produce(produce),
            _ => unimplemented!()
        }
    }

    fn handle_produce(&mut self, produce: ProduceEvent) -> ConnectionHandlerResult {
        debug_assert!(self.produce_operation.is_none());
        let op_id = produce.op_id;

        let partition = self.event_stream.get_partition(0).unwrap();
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
        self.client_sender.send(ProtocolMessage::StreamStatus(status)).map_err(|err| {
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

    fn send_to_client(&self, message: ProtocolMessage) -> ConnectionHandlerResult {
        self.client_sender.send(message).map_err(|e| {
            format!("Error sending outgoing message for connection_id: {}, message: {:?}", self.connection_id, e.into_message())
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

impl <C: Sender<ProtocolMessage>> Sink for ConnectionHandler<C> {
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

        Ok(Async::Ready(()))
    }
}


impl <C: Sender<ProtocolMessage>> Debug for ConnectionHandler<C> {
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

    use super::*;
    use protocol::*;
    use event::ActorId;
    use new_engine::event_stream::partition::*;
    use new_engine::{ClientSender, ClientReceiver, create_client_channels};
    use channels::{Sender, MockSender};
    use atomics::{AtomicCounterWriter, AtomicCounterReader, AtomicBoolReader, AtomicBoolWriter};

    type TestConnectionHandler = ConnectionHandler<MockSender<ProtocolMessage>>;

    struct Fixture {
        partition_receivers: HashMap<(String, ActorId), PartitionReceiver>,
        engine: EngineRef,
    }

    impl Fixture {
        fn create() -> (TestConnectionHandler, Fixture) {
            let client_sender = MockSender::new();
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

            let subject = ConnectionHandler::new(456, client_sender, engine.clone());

            let mut partition_receivers = HashMap::new();
            partition_receivers.insert((system_stream_name(), 1), rx);

            let fixture = Fixture {
                partition_receivers: partition_receivers,
                engine: engine,
            };
            (subject, fixture)
        }

        fn with_stream(stream_name: &str, partition_count: ActorId) -> (TestConnectionHandler, Fixture) {
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

        subject.client_sender.assert_message_sent(ProtocolMessage::StreamStatus(expected));
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

        subject.client_sender.assert_message_sent(ProtocolMessage::Error(expected));
    }
}

