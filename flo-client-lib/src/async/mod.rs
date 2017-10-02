pub mod recv;
pub mod send;

pub mod ops;

use std::error::Error;
use std::time::Duration;
use std::collections::VecDeque;
use std::io;
use std::fmt::{self, Debug};

use futures::{Future, Async, Poll, Stream, Sink, AsyncSink, StartSend};

use protocol::{ProtocolMessage, ErrorMessage};
use event::FloEventId;
use codec::EventCodec;
use self::recv::{MessageStream, MessageRecvStream};
use self::send::{MessageSink, MessageSendSink};
use self::ops::{SendMessages, AwaitResponse, ProduceOne};

pub type MessageSender = Box<Sink<SinkItem=ProtocolMessage, SinkError=io::Error>>;
pub type MessageReceiver = Box<Stream<Item=ProtocolMessage, Error=io::Error>>;

pub const DEFAULT_RECV_BATCH_SIZE: usize = 1000;

pub struct AsyncClient<D: Debug> {
    recv_batch_size: Option<usize>,
    send: Option<MessageSender>,
    recv: Option<MessageReceiver>,
    codec: Box<EventCodec<EventData=D>>,
    current_op_id: u32,
    received_message_buffer: VecDeque<ProtocolMessage>,
}

impl <D: Debug> Debug for AsyncClient<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncClient{{  current_op_id: {}  }}", self.current_op_id)
    }
}


impl <D: Debug> AsyncClient<D> {
    pub fn new(send: MessageSender, recv: MessageReceiver, codec: Box<EventCodec<EventData=D>>) -> AsyncClient<D> {
        AsyncClient {
            recv_batch_size: None,
            send: Some(send),
            recv: Some(recv),
            codec: codec,
            current_op_id: 0,
            received_message_buffer: VecDeque::with_capacity(8),
        }
    }

    pub fn produce(self, namespace: String, parent_id: Option<FloEventId>, data: D) -> ProduceOne<D> {
        ProduceOne::new(self, namespace, parent_id, data)
    }


    fn send_messages(self, messages: Vec<ProtocolMessage>) -> SendMessages<D> {
        SendMessages::new(self, messages)
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

}


#[derive(Debug)]
pub enum ErrorType {
    Codec(Box<Error>),
    Io(io::Error),
    Server(ErrorMessage)
}


