pub mod recv;
pub mod send;
pub mod ops;

mod event_stream_status;
mod tcp_connect;

use std::error::Error;
use std::time::Duration;
use std::collections::VecDeque;
use std::io;
use std::fmt::{self, Debug};
use std::net::{SocketAddr, ToSocketAddrs};

use tokio_core::net::TcpStream;
use tokio_core::io::{ReadHalf, WriteHalf, Io};
use futures::{Future, Async, Poll, Stream, Sink, AsyncSink, StartSend};

use protocol::{ProtocolMessage, ErrorMessage};
use event::{FloEventId, VersionVector};
use codec::EventCodec;
use self::recv::{MessageStream, MessageRecvStream};
use self::send::{MessageSink, MessageSendSink};
use self::ops::{SendMessages, AwaitResponse, ProduceOne, Consume, RequestResponse, ConnectAsyncClient};

pub use self::event_stream_status::{EventStreamStatus, PartitionStatus};
pub type MessageSender = Box<Sink<SinkItem=ProtocolMessage, SinkError=io::Error>>;
pub type MessageReceiver = Box<Stream<Item=ProtocolMessage, Error=io::Error>>;

pub const DEFAULT_RECV_BATCH_SIZE: usize = 1000;

pub struct AsyncClient<D: Debug> {
    client_name: String,
    recv_batch_size: Option<usize>,
    send: Option<MessageSender>,
    recv: Option<MessageReceiver>,
    codec: Box<EventCodec<EventData=D>>,
    current_stream: Option<EventStreamStatus>,
    current_op_id: u32,
    received_message_buffer: VecDeque<ProtocolMessage>,
}

impl <D: Debug> Debug for AsyncClient<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncClient{{  current_op_id: {}  }}", self.current_op_id)
    }
}


impl <D: Debug> AsyncClient<D> {

    pub fn from_tcp_stream(name: String, tcp_stream: TcpStream, codec: Box<EventCodec<EventData=D>>) -> io::Result<AsyncClient<D>> {
        tcp_stream.set_nodelay(true)?; // TODO: perhaps there's a better place to set this. Should we allow the caller to leave Nagle enabled?
        let (tcp_read, tcp_write) = tcp_stream.split();
        let send_sink = MessageSendSink::new(tcp_write);
        let read_stream = MessageRecvStream::new(tcp_read);

        Ok(AsyncClient::new(name, Box::new(send_sink) as MessageSender, Box::new(read_stream) as MessageReceiver, codec))
    }

    pub fn new(name: String, send: MessageSender, recv: MessageReceiver, codec: Box<EventCodec<EventData=D>>) -> AsyncClient<D> {
        AsyncClient {
            client_name: name,
            recv_batch_size: None,
            send: Some(send),
            recv: Some(recv),
            codec: codec,
            current_stream: None,
            current_op_id: 0,
            received_message_buffer: VecDeque::with_capacity(8),
        }
    }

    pub fn produce<N: Into<String>>(self, namespace: N, parent_id: Option<FloEventId>, data: D) -> ProduceOne<D> {
        ProduceOne::new(self, namespace.into(), parent_id, data)
    }

    pub fn consume<N: Into<String>>(self, namespace: N, version_vector: &VersionVector, event_limit: Option<u64>) -> Consume<D> {
        Consume::new(self, namespace.into(), version_vector, event_limit)
    }

    pub fn connect(self) -> ConnectAsyncClient<D> {
        ConnectAsyncClient::new(self)
    }


    fn await_response(self, op_id: u32) -> AwaitResponse<D> {
        AwaitResponse::new(self, op_id)
    }

    fn can_buffer_received(&self) -> bool {
        let max_buffered = self.recv_batch_size.unwrap_or(DEFAULT_RECV_BATCH_SIZE);
        self.received_message_buffer.len() < max_buffered
    }

    fn buffer_received(&mut self, message: ProtocolMessage) {
        self.received_message_buffer.push_back(message);
    }

    fn next_op_id(&mut self) -> u32 {
        self.current_op_id += 1;
        self.current_op_id
    }

}


#[derive(Debug)]
pub enum ErrorType {
    Codec(Box<Error>),
    Io(io::Error),
    Server(ErrorMessage)
}



#[cfg(test)]
mod test {
    use super::*;

    use std::time::Duration;
    use std::io;
    use std::sync::{Arc, Mutex};

    use futures::{Stream, Async, Poll, AsyncSink};

    use protocol::{ProtocolMessage, EventAck};
    use event::*;
    use codec::{EventCodec, StringCodec};

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

    pub struct MockSinkVerifier(Arc<Mutex<Vec<ProtocolMessage>>>);

    impl MockSinkVerifier {
        fn get_received(&mut self) -> Vec<ProtocolMessage> {
            let mut vec = self.0.lock().unwrap();


            let result = ::std::mem::replace(vec.as_mut(), Vec::new());
            let mut result = result;
            result.reverse();
            result
        }
    }

    pub struct MockSendStream {
        received: Arc<Mutex<Vec<ProtocolMessage>>>,
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
        type SinkItem = ProtocolMessage;
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
    }


    pub struct MockReceiveStream{
        to_produce: Vec<ProtocolMessage>,
        state: MockState,
    }

    impl MockReceiveStream {
        pub fn will_produce(mut messages: Vec<ProtocolMessage>) -> MessageReceiver {
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
        type Item = ProtocolMessage;
        type Error = io::Error;

        fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
            if self.state.flip().ready() {
                Ok(Async::Ready(self.to_produce.pop()))
            } else {
                Ok(Async::NotReady)
            }
        }
    }

    fn create_client(recv: MessageReceiver, send: MessageSender) -> AsyncClient<String> {
        AsyncClient::new("testClient".to_owned(), send, recv, Box::new(StringCodec) as Box<EventCodec<EventData=String>>)
    }

    fn run_future<T, E, F>(mut future: F) -> Result<T, E> where F: Future<Item=T, Error=E> {
        use tokio_core::reactor::{Core};
        use std::time::Instant;

        let start = Instant::now();
        let timeout = Duration::from_millis(100);

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
    fn send_all_sends_all_messages() {
        let recv = MockReceiveStream::empty();
        let (send, mut send_verify) = MockSendStream::new();
        let mut client = create_client(recv, send);

        let messages = vec![
            ProtocolMessage::NextBatch,
            ProtocolMessage::EndOfBatch,
            ProtocolMessage::AckEvent(EventAck { op_id: 7, event_id: FloEventId::new(8, 9) }),
        ];
        let send_all = SendMessages::new(client, messages.clone());

        let result = run_future(send_all).expect("failed to run send_all");

        assert_eq!(messages, send_verify.get_received());
    }

    #[test]
    fn await_response_returns_matching_message_and_buffers_others() {
        let messages = vec![
            ProtocolMessage::EndOfBatch,
            ProtocolMessage::NextBatch,
            ProtocolMessage::AckEvent(EventAck { op_id: 7, event_id: FloEventId::new(8, 9) }),
        ];

        let recv = MockReceiveStream::will_produce(messages.clone());
        let (send, mut send_verify) = MockSendStream::new();
        let client = create_client(recv, send);

        let await = AwaitResponse::new(client, 7);
        let (response, client): (ProtocolMessage, AsyncClient<String>) = run_future(await).expect("await response returned error");
        assert_eq!(ProtocolMessage::AckEvent(EventAck { op_id: 7, event_id: FloEventId::new(8, 9) }), response);

        let expected_buffer = vec![
            ProtocolMessage::EndOfBatch,
            ProtocolMessage::NextBatch,
        ];
        let actual_buffer: Vec<ProtocolMessage> = client.received_message_buffer.iter().cloned().collect();
        assert_eq!(expected_buffer, actual_buffer);
    }

    #[test]
    fn consume_yields_stream_of_events() {
        use protocol::{CursorInfo, RecvEvent};
        use event::{OwnedFloEvent, VersionVector, time};
        use ::Event;

        let consume_op_id = 999;

        let to_receive = vec![
            ProtocolMessage::CursorCreated(CursorInfo{ op_id: consume_op_id, batch_size: 1 }),
            ProtocolMessage::ReceiveEvent(RecvEvent::Owned(OwnedFloEvent {
                id: FloEventId::new(3, 4),
                timestamp: time::from_millis_since_epoch(8),
                parent_id: None,
                namespace: "/foo/bar".to_owned(),
                data: "first event data".as_bytes().to_owned(),
            })),
            ProtocolMessage::EndOfBatch,
            ProtocolMessage::AwaitingEvents,
            ProtocolMessage::ReceiveEvent(RecvEvent::Owned(OwnedFloEvent {
                id: FloEventId::new(3, 5),
                timestamp: time::from_millis_since_epoch(9),
                parent_id: Some(FloEventId::new(3, 4)),
                namespace: "/foo/bar".to_owned(),
                data: "second event data".as_bytes().to_owned(),
            })),
        ];
        let receiver = MockReceiveStream::will_produce(to_receive);
        let (sender, send_verify) = MockSendStream::new();
        let mut client = create_client(receiver, sender);

        // setup the client so that the next op_id will be `consume_op_id`
        client.current_op_id = consume_op_id - 1;

        let mut version_vec = VersionVector::new();
        version_vec.set(FloEventId::new(1, 2));
        version_vec.set(FloEventId::new(2, 8));
        version_vec.set(FloEventId::new(3, 4));

        let consume_stream = client.consume("/foo/*", &version_vec, Some(2));
        let results = get_stream_results(consume_stream);
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
