pub mod recv;
pub mod send;
pub mod ops;

mod current_stream_state;
mod tcp_connect;

use std::error::Error;
use std::collections::VecDeque;
use std::io;
use std::fmt::{self, Debug};

use tokio_core::net::TcpStream;
#[allow(deprecated)]
use tokio_core::io::Io;
use futures::{Stream, Sink};

use protocol::{ProtocolMessage, ErrorMessage};
use event::{FloEventId, ActorId, VersionVector, OwnedFloEvent};
use codec::EventCodec;
use self::recv::MessageRecvStream;
use self::send::MessageSendSink;
use self::ops::{ProduceOne, ProduceAll, EventToProduce, Consume, Handshake, SetEventStream};


pub use self::tcp_connect::{tcp_connect, tcp_connect_with, AsyncTcpClientConnect};
pub use self::current_stream_state::{CurrentStreamState, PartitionState};

pub type ClientProtocolMessage = ProtocolMessage<OwnedFloEvent>;
pub type MessageSender = Box<Sink<SinkItem=ClientProtocolMessage, SinkError=io::Error>>;
pub type MessageReceiver = Box<Stream<Item=ClientProtocolMessage, Error=io::Error>>;

pub const DEFAULT_RECV_BATCH_SIZE: u32 = 1000;


/// Represents a single connection to a flo server.
/// All operations return a `Future` representing the result of the operation. The actual operation will not be performed
/// until the `Future` is driven to completion by an `Executor`. Most of these futures will yield a result containing both
/// the desired result and the connection itself.
///
/// An `AsyncConnection` uses an `EventCodec` to convert between an application's event data type and the binary data used by flo.
/// The transport layer is abstracted, allowing the connection to work over TCP or use in memory channels (for an embedded server).
///
pub struct AsyncConnection<D: Debug> {
    inner: Box<AsyncConnectionInner<D>>,
}

impl <D: Debug> Debug for AsyncConnection<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncConnection{{  current_op_id: {}  }}", self.inner.current_op_id)
    }
}


impl <D: Debug> AsyncConnection<D> {

    /// Creates a new connection from an already connected `TCPStream`. Generally, you should prefer to just use the `tcp_connect`
    /// function, but using `from_tcp_stream` is available for when extra control is needed in how the tcp stream is
    /// configured.
    pub fn from_tcp_stream(name: String, tcp_stream: TcpStream, codec: Box<EventCodec<EventData=D>>) -> AsyncConnection<D> {
        #[allow(deprecated)] // TODO: maybe migrate to tokio-io crate? but that'll be deprecated soon anyway
        let (tcp_read, tcp_write) = tcp_stream.split();
        let send_sink = MessageSendSink::new(tcp_write);
        let read_stream = MessageRecvStream::new(tcp_read);

        AsyncConnection::new(name, Box::new(send_sink) as MessageSender, Box::new(read_stream) as MessageReceiver, codec)
    }

    /// Creates a new AsyncConnection from raw parts
    pub fn new(name: String, send: MessageSender, recv: MessageReceiver, codec: Box<EventCodec<EventData=D>>) -> AsyncConnection<D> {
        let inner = AsyncConnectionInner {
            client_name: name,
            recv_batch_size: None,
            send: Some(send),
            recv: Some(recv),
            codec: codec,
            current_stream: None,
            current_op_id: 0,
            received_message_buffer: VecDeque::with_capacity(8),
        };
        AsyncConnection {
            inner: Box::new(inner)
        }
    }

    /// If this connection has successfully connected to the server, then this will return the most recent `CurrentStreamState`
    /// received from the server. If the connection has never been completed, then this will return `None`.
    /// Note that the `CurrentStreamState` may not necessarily be up to date with the most recent state of the server, as
    /// this function does not actually query the state, but just returns the result from the last query or handshake.
    pub fn current_stream(&self) -> Option<&CurrentStreamState> {
        self.inner.current_stream.as_ref()
    }

    /// Sets the event stream to use with this connection. If unset, then the default stream configured in the server will
    /// be used. If the returned future completed successfully, then the connection will use the given event stream for all
    /// operations from this point forward, and `current_steam` will return a `CurrentStreamState` corresponding to the given
    /// stream.
    pub fn set_event_stream<S: Into<String>>(self, new_stream: S) -> SetEventStream<D> {
        SetEventStream::new(self, new_stream.into())
    }

    /// Produce a single event on the stream and await acknowledgement that it was persisted. Returns a future that resolves
    /// to a tuple of the `FloEventId` of the produced event and this `AsyncConnection`.
    pub fn produce(self, event: EventToProduce<D>) -> ProduceOne<D> {
        let EventToProduce{partition, namespace, parent_id, data} = event;
        self.produce_to(partition, namespace, parent_id, data)
    }

    /// Produces a single event to the specified partition and awaits acknowledgement that it was persisted. Returns a future
    /// that resolves to a tuple of the `FloEventId` of the new event and this `AsyncConnection` for reuse.
    pub fn produce_to<N: Into<String>>(self, partition: ActorId, namespace: N, parent_id: Option<FloEventId>, data: D) -> ProduceOne<D> {
        ProduceOne::new(self, partition, namespace.into(), parent_id, data)
    }

    /// Produce each of the events yielded by the iterator. The events are each produced in order. Subsequent operations are not
    /// begun until the previous event has been acknowledged as being persisted successfully. In the event of a failure, all
    /// subsequent events are skipped, and the error is returned immediated. The error struct contains the number of events
    /// that were produced successfully.
    pub fn produce_all<I: Iterator<Item=EventToProduce<D>>>(self, events: I) -> ProduceAll<D, I> {
        ProduceAll::new(self, events)
    }

    /// Start consuming events from the server. Returns a `Stream` that yields events continuously until the `event_limit` is reached.
    /// If `event_limit` is `None`, then the resulting `Stream` will never terminate unless there's an error.
    /// The `version_vector` represents the exclusive starting `EventCounter` for each partition on the stream that the consumer
    /// will receive events for. Only events matching the `namespace` glob will be received.
    pub fn consume<N: Into<String>>(self, namespace: N, version_vector: &VersionVector, event_limit: Option<u64>, await_new: bool) -> Consume<D> {
        Consume::new(self, namespace.into(), version_vector, event_limit, await_new)
    }

    /// Initiates the handshake with the server. The returned `Future` resolves the this connection, which will then be guaranteed
    /// to have the `current_stream()` return `Some`.
    pub fn connect(self) -> Handshake<D> {
        self.connect_with(None)
    }

    /// Initiates the handshake with the server, using the given `consume_batch_size`. The returned `Future`
    /// resolves to this connection, which will then be guaranteed to have `current_stream()` return
    /// `Some` as long as the handshake was successful.
    pub fn connect_with(mut self, consume_batch_size: Option<u32>) -> Handshake<D> {
        self.inner.recv_batch_size = consume_batch_size;
        Handshake::new(self)
    }

    fn take_sender(&mut self) -> MessageSender {
        self.inner.send.take().unwrap()
    }

    fn return_sender(&mut self, send: MessageSender) {
        self.inner.send = Some(send);
    }

    fn can_buffer_received(&self) -> bool {
        let max_buffered = self.inner.recv_batch_size.unwrap_or(DEFAULT_RECV_BATCH_SIZE);
        self.inner.received_message_buffer.len() < max_buffered as usize
    }

    fn buffer_received(&mut self, message: ClientProtocolMessage) {
        self.inner.received_message_buffer.push_back(message);
    }

    fn next_op_id(&mut self) -> u32 {
        self.inner.current_op_id += 1;
        self.inner.current_op_id
    }
}


#[derive(Debug)]
pub enum ErrorType {
    Codec(Box<Error>),
    Io(io::Error),
    Server(ErrorMessage)
}

impl ErrorType {
    pub fn unexpected_message(expected: &'static str, actual: ClientProtocolMessage) -> ErrorType {
        let msg = format!("Unexpected message: {:?}, expected: {}", actual, expected);
        io::Error::new(io::ErrorKind::InvalidData, msg).into()
    }
}

impl From<ErrorMessage> for ErrorType {
    fn from(message: ErrorMessage) -> Self {
        ErrorType::Server(message)
    }
}

impl From<io::Error> for ErrorType {
    fn from(io_err: io::Error) -> Self {
        ErrorType::Io(io_err)
    }
}

/// a connection is a fairly large struct with a lot of state. `AsyncConnectionInner` is allocated on the heap
/// so that moves on a connection object become very cheap (the `AsyncConnection` itself is just a pointer).
/// This also makes it easy to allow various ops to have access to internal connection state while preventing users
/// from accessing it.
struct AsyncConnectionInner<D: Debug> {
    client_name: String,
    recv_batch_size: Option<u32>,
    send: Option<MessageSender>,
    recv: Option<MessageReceiver>,
    codec: Box<EventCodec<EventData=D>>,
    current_stream: Option<CurrentStreamState>,
    current_op_id: u32,
    received_message_buffer: VecDeque<ClientProtocolMessage>,
}



#[cfg(test)]
mod test {
    use super::*;

    use std::io;
    use std::sync::{Arc, Mutex};

    use futures::{Stream, Async, Poll, AsyncSink, StartSend, Future};

    use protocol::*;
    use codec::{EventCodec, StringCodec};
    use super::ops::*;


    #[derive(Copy, Clone, PartialEq)]
    enum MockState {
        NotReady,
        Ready,
    }

    impl MockState {
        fn flip(&mut self) -> MockState {
            match *self {
                MockState::Ready => {
                    *self = MockState::NotReady;
                }
                MockState::NotReady => {
                    *self = MockState::Ready;
                }
            }
            *self
        }

        fn ready(&self) -> bool {
            match *self {
                MockState::Ready => true,
                _ => false
            }
        }
    }

    pub struct MockSinkVerifier(Arc<Mutex<Vec<ClientProtocolMessage>>>);

    impl MockSinkVerifier {
        fn get_received(&mut self) -> Vec<ClientProtocolMessage> {
            let mut vec = self.0.lock().unwrap();


            ::std::mem::replace(vec.as_mut(), Vec::new())
        }
    }

    pub struct MockSendStream {
        received: Arc<Mutex<Vec<ClientProtocolMessage>>>,
        state: MockState,
    }

    impl MockSendStream {
        pub fn new() -> (MessageSender, MockSinkVerifier) {
            let recv = Arc::new(Mutex::new(Vec::new()));
            let mock_send = MockSendStream {
                received: recv.clone(),
                state: MockState::NotReady
            };
            let verify = MockSinkVerifier(recv);
            (Box::new(mock_send) as MessageSender, verify)
        }
    }

    impl Sink for MockSendStream {
        type SinkItem = ClientProtocolMessage;
        type SinkError = io::Error;

        fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
            match self.state {
                MockState::Ready => {
                    self.state = MockState::NotReady;
                    let mut vec = self.received.lock().unwrap();
                    vec.push(item);
                    Ok(AsyncSink::Ready)
                }
                MockState::NotReady => {
                    self.state = MockState::Ready;
                    Ok(AsyncSink::NotReady(item))
                }
            }
        }

        fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
            if self.state.flip().ready() {
                Ok(Async::Ready(()))
            } else {
                Ok(Async::NotReady)
            }
        }

        fn close(&mut self) -> Poll<(), Self::SinkError> {
            self.poll_complete()
        }
    }


    pub struct MockReceiveStream{
        to_produce: Vec<ClientProtocolMessage>,
        state: MockState,
    }

    impl MockReceiveStream {
        pub fn will_produce(mut messages: Vec<ClientProtocolMessage>) -> MessageReceiver {
            // Vec::pop removes from the end of the vec, but the tests are more readable if messages are yielded in the order that they are in the vec
            messages.reverse();
            Box::new(MockReceiveStream {
                to_produce: messages,
                state: MockState::NotReady,
            }) as MessageReceiver
        }

        pub fn empty() -> MessageReceiver {
            MockReceiveStream::will_produce(Vec::new())
        }
    }

    impl Stream for MockReceiveStream {
        type Item = ClientProtocolMessage;
        type Error = io::Error;

        fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
            if self.state.flip().ready() {
                Ok(Async::Ready(self.to_produce.pop()))
            } else {
                Ok(Async::NotReady)
            }
        }
    }

    fn create_client(recv: MessageReceiver, send: MessageSender) -> AsyncConnection<String> {
        AsyncConnection::new("testClient".to_owned(), send, recv, Box::new(StringCodec) as Box<EventCodec<EventData=String>>)
    }

    fn run_future<T, E, F>(mut future: F) -> Result<T, E> where F: Future<Item=T, Error=E> {
        let mut poll_countdown = 20;
        while poll_countdown > 0 {
            poll_countdown -= 1;
            let result = future.poll();
            match result {
                Ok(Async::Ready(value)) => return Ok(value),
                Err(err) => return Err(err),
                _ => { }
            }
        }
        panic!("future never returned a value");
    }

    fn get_stream_results<T, E, S>(mut stream: S) -> Vec<T> where S: Stream<Item=T, Error=E>, E: Debug {
        let mut results = Vec::new();

        let mut poll_countdown = 20;

        while poll_countdown > 0 {
            let async = stream.poll().expect("poll returned error");
            match async {
                Async::Ready(Some(t)) => {
                    results.push(t);
                }
                Async::Ready(None) => {
                    poll_countdown = 0;
                },
                Async::NotReady => {
                    poll_countdown -= 1;
                }
            }
        }
        results
    }

    #[test]
    fn produce_all_produces_multiple_events_in_sequence() {
        let expected_sent = vec![
            ProtocolMessage::ProduceEvent(ProduceEvent::with_crc(
                1,
                1,
                "/foo".to_owned(),
                None,
                Vec::new(),
            )),
            ProtocolMessage::ProduceEvent(ProduceEvent::with_crc(
                2,
                2,
                "/bar".to_owned(),
                None,
                Vec::new(),
            )),
            ProtocolMessage::ProduceEvent(ProduceEvent::with_crc(
                3,
                3,
                "/baz".to_owned(),
                None,
                Vec::new(),
            ))
        ];
        let to_recv = vec![
            ProtocolMessage::AckEvent(EventAck{
                op_id: 1,
                event_id: FloEventId::new(1, 1),
            }),
            ProtocolMessage::AckEvent(EventAck{
                op_id: 2,
                event_id: FloEventId::new(2, 2),
            }),
            ProtocolMessage::AckEvent(EventAck{
                op_id: 3,
                event_id: FloEventId::new(3, 3),
            }),
        ];

        let events_to_produce = vec![
            EventToProduce {
                partition: 1,
                namespace: "/foo".to_owned(),
                parent_id: None,
                data: String::new()
            },
            EventToProduce {
                partition: 2,
                namespace: "/bar".to_owned(),
                parent_id: None,
                data: String::new()
            },
            EventToProduce {
                partition: 3,
                namespace: "/baz".to_owned(),
                parent_id: None,
                data: String::new()
            }
        ];

        let recv = MockReceiveStream::will_produce(to_recv);
        let (send, mut send_verify) = MockSendStream::new();
        let connection = create_client(recv, send);

        let op = connection.produce_all(events_to_produce.into_iter());
        let result = run_future(op).expect("failed to run produce_all");

        assert_eq!(expected_sent, send_verify.get_received());

        let expected_ids = vec![
            FloEventId::new(1, 1),
            FloEventId::new(2, 2),
            FloEventId::new(3, 3)
        ];
        assert_eq!(expected_ids, result.events_produced);
    }

    #[test]
    fn produce_all_returns_immediate_success_when_iterator_is_empty() {
        let recv = MockReceiveStream::empty();
        let (send, mut send_verify) = MockSendStream::new();
        let connection = create_client(recv, send);

        let mut op = connection.produce_all(Vec::new().into_iter());

        let result = op.poll().unwrap();

        match result {
            Async::Ready(result) => {
                assert!(result.events_produced.is_empty());
            }
            Async::NotReady => {
                panic!("Expected Async::Ready but got NotReady");
            }
        }

        assert!(send_verify.get_received().is_empty());
    }

    #[test]
    fn connect_initiates_connection() {
        let to_recv = vec![ProtocolMessage::StreamStatus(EventStreamStatus {
            op_id: 1,
            name: "foo".to_owned(),
            partitions: vec![
                PartitionStatus {
                    partition_num: 1,
                    head: 7,
                    primary: true,
                    primary_server_address: None,
                },
                PartitionStatus {
                    partition_num: 2,
                    head: 5,
                    primary: false,
                    primary_server_address: None,
                }
            ],
        })];
        let recv = MockReceiveStream::will_produce(to_recv);
        let (send, _send_verify) = MockSendStream::new();
        let connection = create_client(recv, send);

        let connect = connection.connect();
        let connection = run_future(connect).expect("failed to execute connect");

        let expected_stream = CurrentStreamState {
            name: "foo".to_owned(),
            partitions: vec![
                PartitionState {
                    partition_num: 1,
                    head: 7,
                    writable: true,
                    primary_server_addr: None,
                },
                PartitionState {
                    partition_num: 2,
                    head: 5,
                    writable: false,
                    primary_server_addr: None,
                }
            ]
        };
        assert_eq!(Some(&expected_stream), connection.current_stream());
    }

    #[test]
    fn send_sends_all_messages() {
        let recv = MockReceiveStream::empty();
        let (send, mut send_verify) = MockSendStream::new();
        let connection = create_client(recv, send);

        let send = SendMessage::new(connection, ProtocolMessage::NextBatch);

        let _ = run_future(send).expect("failed to run send");
        assert_eq!(vec![ProtocolMessage::NextBatch], send_verify.get_received());
    }

    #[test]
    fn await_response_returns_matching_message_and_buffers_others() {
        let messages = vec![
            ProtocolMessage::EndOfBatch,
            ProtocolMessage::NextBatch,
            ProtocolMessage::AckEvent(EventAck { op_id: 7, event_id: FloEventId::new(8, 9) }),
        ];

        let recv = MockReceiveStream::will_produce(messages.clone());
        let (send, _send_verify) = MockSendStream::new();
        let connection = create_client(recv, send);

        let await = AwaitResponse::new(connection, 7);
        let (response, connection): (ClientProtocolMessage, AsyncConnection<String>) = run_future(await).expect("await response returned error");
        assert_eq!(ProtocolMessage::AckEvent(EventAck { op_id: 7, event_id: FloEventId::new(8, 9) }), response);

        let expected_buffer = vec![
            ProtocolMessage::EndOfBatch,
            ProtocolMessage::NextBatch,
        ];
        let actual_buffer: Vec<ClientProtocolMessage> = connection.inner.received_message_buffer.iter().cloned().collect();
        assert_eq!(expected_buffer, actual_buffer);
    }

    #[test]
    fn consume_yields_stream_of_events() {
        use protocol::CursorInfo;
        use event::{OwnedFloEvent, VersionVector, FloEventId, time};
        use ::Event;

        let consume_op_id = 999;

        let to_receive = vec![
            ProtocolMessage::CursorCreated(CursorInfo{ op_id: consume_op_id, batch_size: 1 }),
            ProtocolMessage::ReceiveEvent(OwnedFloEvent::new(
                FloEventId::new(3, 4),
                None,
                time::from_millis_since_epoch(8),
                "/foo/bar".to_owned(),
                "first event data".as_bytes().to_owned(),
            )),
            ProtocolMessage::EndOfBatch,
            ProtocolMessage::AwaitingEvents,
            ProtocolMessage::ReceiveEvent(OwnedFloEvent::new(
                FloEventId::new(3, 5),
                Some(FloEventId::new(3, 4)),
                time::from_millis_since_epoch(9),
                "/foo/bar".to_owned(),
                "second event data".as_bytes().to_owned(),
            )),
        ];
        let receiver = MockReceiveStream::will_produce(to_receive);
        let (sender, mut send_verify) = MockSendStream::new();
        let mut connection = create_client(receiver, sender);

        // setup the connection so that the next op_id will be `consume_op_id`
        connection.inner.current_op_id = consume_op_id - 1;

        let mut version_vec = VersionVector::new();
        version_vec.set(FloEventId::new(1, 2));
        version_vec.set(FloEventId::new(2, 8));
        version_vec.set(FloEventId::new(3, 4));

        let consume_stream = connection.consume("/foo/*", &version_vec, Some(2), true);
        let results = get_stream_results(consume_stream);

        let sent = send_verify.get_received();
        let expected_sent = vec![
            ProtocolMessage::NewStartConsuming(NewConsumerStart {
                op_id: consume_op_id,
                options: Default::default(),
                version_vector: vec![FloEventId::new(1, 2), FloEventId::new(2, 8), FloEventId::new(3, 4)],
                max_events: 2,
                namespace: "/foo/*".to_owned(),
            }),
            ProtocolMessage::NextBatch,
        ];
        assert_eq!(expected_sent, sent);

        let expected = vec![
            Event {
                id: FloEventId::new(3, 4),
                timestamp: time::from_millis_since_epoch(8),
                parent_id: None,
                namespace: "/foo/bar".to_owned(),
                data: "first event data".to_owned(),
            },
            Event {
                id: FloEventId::new(3, 5),
                timestamp: time::from_millis_since_epoch(9),
                parent_id: Some(FloEventId::new(3, 4)),
                namespace: "/foo/bar".to_owned(),
                data: "second event data".to_owned(),
            }
        ];
        assert_eq!(expected, results);
    }
}
