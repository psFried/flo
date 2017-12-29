pub mod connection_state;
mod consumer;
mod producer;
mod peer;
mod input;

use std::fmt::{self, Debug};
use std::io;

#[allow(unused_imports)]
use futures::{Async, Poll, AsyncSink, StartSend, Sink, Stream, Future};
use tokio_core::reactor::Handle;

use protocol::*;
use engine::{ConnectionId, ClientSender, EngineRef, ReceivedProtocolMessage};
use self::connection_state::ConnectionState;
use self::consumer::ConsumerConnectionState;
use self::producer::ProducerConnectionState;
use self::peer::PeerConnectionState;

pub use self::input::{ConnectionHandlerInput, ConnectionControl};

pub type ConnectionControlSender = ::futures::sync::mpsc::UnboundedSender<ConnectionControl>;
pub type ConnectionControlReceiver = ::futures::sync::mpsc::UnboundedReceiver<ConnectionControl>;

pub fn create_connection_control_channels() -> (ConnectionControlSender, ConnectionControlReceiver) {
    ::futures::sync::mpsc::unbounded()
}

pub struct ConnectionHandler {
    common_state: ConnectionState,
    consumer_state: ConsumerConnectionState,
    producer_state: ProducerConnectionState,
    peer_state: PeerConnectionState,
}


pub type ConnectionHandlerResult = Result<(), String>;

impl ConnectionHandler {
    pub fn new(connection: ConnectionId, client_sender: ClientSender, engine: EngineRef, handle: Handle) -> ConnectionHandler {
        ConnectionHandler {
            common_state: ConnectionState::new(connection, client_sender, engine, handle),
            consumer_state: ConsumerConnectionState::new(),
            producer_state: ProducerConnectionState::new(),
            peer_state: PeerConnectionState::Init,
        }
    }

    pub fn upgrade_to_outgoing_peer(&mut self) {
        debug!("upgrading connection_id: {} to outgoing peer connection", self.common_state.connection_id);
        let ConnectionHandler{ref mut common_state, ref mut peer_state, .. } = *self;

        peer_state.initiate_outgoing_peer_connection(common_state);
    }

    pub fn can_process(&self, _message: &ReceivedProtocolMessage) -> bool {
        !self.producer_state.requires_poll_complete() && !self.consumer_state.requires_poll_complete()
    }

    pub fn handle_control(&mut self, control: ConnectionControl) -> ConnectionHandlerResult {
        debug!("client: {:?} processing control: {:?}", self.common_state, control);

        match control {
            ConnectionControl::InitiateOutgoingSystemConnection => {
                self.upgrade_to_outgoing_peer()
            }
        }
        Ok(())
    }

    pub fn handle_incoming_message(&mut self, message: ReceivedProtocolMessage) -> ConnectionHandlerResult {
        trace!("client: {:?}, received message: {:?}", self.common_state, message);

        let ConnectionHandler{ref mut common_state, ref mut consumer_state, ref mut producer_state, ref mut peer_state } = *self;

        match message {
            ProtocolMessage::SetEventStream(SetEventStream{op_id, name}) => {
                common_state.set_event_stream(op_id, name)
            },
            ProtocolMessage::Announce(announce) => {
                common_state.handle_announce_message(announce)
            },
            ProtocolMessage::PeerAnnounce(peer_announce) => {
                peer_state.peer_announce_received(peer_announce, common_state)
            }
            ProtocolMessage::ProduceEvent(produce) => {
                producer_state.handle_produce(produce, common_state)
            },
            ProtocolMessage::NewStartConsuming(consumer_start) => {
                consumer_state.handle_start_consuming(consumer_start, common_state)
            },
            ProtocolMessage::NextBatch => {
                consumer_state.handle_next_batch(common_state)
            }
            ProtocolMessage::StopConsuming(op_id) => {
                consumer_state.stop_consuming(op_id, common_state)
            }
            _ => unimplemented!()
        }
    }

}



impl Sink for ConnectionHandler {
    type SinkItem = ConnectionHandlerInput;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match item {
            ConnectionHandlerInput::IncomingMessage(message) => {
                if !self.can_process(&message) {
                    return Ok(AsyncSink::NotReady(message.into()));
                }

                self.handle_incoming_message(message)
            }
            ConnectionHandlerInput::Control(control) => {
                self.handle_control(control)
            }

        }.map(|()| {
            AsyncSink::Ready
        }).map_err(|err_string| {
            io::Error::new(io::ErrorKind::Other, err_string)
        })
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let ConnectionHandler {ref mut common_state, ref mut consumer_state, ref mut producer_state, ..} = *self;

        if producer_state.requires_poll_complete() {
            producer_state.poll_produce_complete(common_state)
        } else if consumer_state.requires_poll_complete() {
            consumer_state.poll_consume_complete(common_state)
        } else {
            Ok(Async::Ready(()))
        }
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        let ConnectionHandler {ref mut common_state, ref mut consumer_state, ..} = *self;
        consumer_state.shutdown(common_state);
    }
}


impl Debug for ConnectionHandler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ConnectionHandler")
                .field("common_state", &self.common_state)
                .field("consumer_state", &self.consumer_state)
                .field("producer_state", &self.producer_state)
                .finish()
    }
}



#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, RwLock};
    use tokio_core::reactor::Core;

    use super::*;
    use event::ActorId;
    use engine::{SYSTEM_STREAM_NAME, system_stream_name};
    use engine::event_stream::EventStreamRef;
    use engine::event_stream::partition::*;
    use engine::ClientReceiver;
    use engine::controller::{SystemStreamRef, SharedClusterState};
    use atomics::{AtomicCounterWriter, AtomicBoolWriter};

    struct Fixture {
        #[allow(dead_code)] // TODO: add more connection handler tests
        partition_receivers: HashMap<(String, ActorId), PartitionReceiver>,
        system_receiver: ::engine::controller::SystemPartitionReceiver,
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
            let primary_addr = Arc::new(RwLock::new(None));

            let (tx, rx) = ::engine::controller::create_system_partition_channels();
            let part_ref = PartitionRef::system(system_stream_name(),
                                             1,
                                             counter_writer.reader(),
                                             primary.reader(),
                                             tx.clone(),
                                             primary_addr);

            let cluster_state = SharedClusterState {
                this_instance_id: FloInstanceId::generate_new(),
                this_address: None,
                system_primary: None,
                peers: Vec::new(),
            };
            let system_stream = SystemStreamRef::new(part_ref, tx, Arc::new(RwLock::new(cluster_state)));

            let streams = Arc::new(Mutex::new(HashMap::new()));
            let engine = EngineRef::new(system_stream, streams);

            let subject = ConnectionHandler::new(456, client_sender, engine.clone(), reactor.handle());

            let fixture = Fixture {
                partition_receivers: HashMap::new(),
                system_receiver: rx,
                client_receiver: Some(client_rx),
                engine: engine,
                reactor: reactor
            };
            (subject, fixture)
        }

        #[allow(dead_code)] // TODO: add more connection handler tests
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
                let primary_addr = Arc::new(RwLock::new(None));
                let (tx, rx) = create_partition_channels();
                let part_ref = PartitionRef::new(name.to_owned(),
                                                 partition_num,
                                                 counter_writer.reader(),
                                                 primary.reader(),
                                                 tx,
                                                 primary_addr);
                partition_refs.push(part_ref);
                self.partition_receivers.insert((name.to_owned(), partition_num), rx);
            }
            partition_refs.sort_by_key(|p| p.partition_num());
            let stream_ref = EventStreamRef::new(name.to_owned(), partition_refs);
            self.engine.event_streams.lock().map(|mut map| {
                map.insert(name.to_owned(), stream_ref)
            }).unwrap();
        }

        #[allow(dead_code)] // TODO: add more connection handler tests
        fn message_sent_to_partition(&self, event_stream: &str, partition_id: ActorId) -> Operation {
            let key = (event_stream.to_owned(), partition_id);
            let partition_receiver = self.partition_receivers.get(&key).expect("no such partition");
            let timeout = ::std::time::Duration::new(0, 0);
            let result = partition_receiver.recv_timeout(timeout);
            result.expect(&format!("partition: {} failed to receive message", partition_id))
        }

        fn assert_sent_to_client(&mut self, expected: ProtocolMessage<PersistentEvent>) {
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
                    primary_server_address: None,
                },
                PartitionStatus {
                    partition_num: 2,
                    head: 0,
                    primary: true,
                    primary_server_address: None,
                },
                PartitionStatus {
                    partition_num: 3,
                    head: 0,
                    primary: true,
                    primary_server_address: None,
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

