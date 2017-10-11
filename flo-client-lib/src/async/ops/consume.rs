
use std::fmt::{self, Debug};
use std::io;

use futures::{Future, Async, Poll, Stream};
use futures::sink::SendAll;

use event::{OwnedFloEvent, VersionVector};
use protocol::{ProtocolMessage, NewConsumerStart, ErrorMessage, RecvEvent};
use async::{AsyncClient, ErrorType};
use async::ops::{SendMessage, SendError, AwaitResponse, AwaitResponseError};
use ::Event;


pub struct Consume<D: Debug> {
    op_id: u32,
    batch_size: u32,
    namespace: String,
    total_events_remaining: Option<u64>,
    state: State<D>,
}

type PollState<D> = Poll<PollSuccess<D>, ConsumeError<D>>;

impl <D: Debug> Consume<D> {

    pub fn new(mut client: AsyncClient<D>, namespace: String, version_vec: &VersionVector, event_limit: Option<u64>) -> Consume<D> {
        let total_event_limit = event_limit.unwrap_or(::std::u64::MAX);
        let op_id = client.next_op_id();
        let consumer_start = NewConsumerStart {
            version_vector: version_vec.snapshot(),
            max_events: event_limit.unwrap_or(0),
            namespace: namespace.clone(),
        };
        let message = ProtocolMessage::NewStartConsuming(consumer_start);
        let initial_state = State::RequestStart(SendMessage::new(client, message));

        Consume {
            op_id: op_id,
            batch_size: 0,
            namespace: namespace,
            total_events_remaining: event_limit,
            state: initial_state
        }
    }

    pub fn decrement_events_remaining(&mut self) {
        if let Some(count) = self.total_events_remaining.as_mut() {
            *count -= 1;
        }
    }

    pub fn event_limit_reached(&self) -> bool {
        self.total_events_remaining.map(|rem| rem == 0).unwrap_or(false)
    }
}

fn response_received<D: Debug>(op_id: u32, response: ProtocolMessage, client: AsyncClient<D>) -> Result<Async<(u32, State<D>)>, ConsumeError<D>> {
    match response {
        ProtocolMessage::CursorCreated(info) => {
            debug!("Consumer with op_id: {} received CursorCreated: {:?}", op_id, info);
            let new_state = State::ReceiveEvents(EventReceiver(Some(client)));
            Ok(Async::Ready((info.batch_size, new_state)))
        }
        other @ _ => {
            warn!("consumer with op_id: {} received error response: {:?}", op_id, other);
            Err(consume_error(client, other))
        }
    }
}

fn new_state<D: Debug>(state: State<D>) -> PollState<D> {
    Ok(Async::Ready(PollSuccess::NewState(state)))
}

fn consume_error<D: Debug>(client: AsyncClient<D>, message: ProtocolMessage) -> ConsumeError<D> {
    match message {
        ProtocolMessage::Error(error_message) => {
            ConsumeError {
                client: client,
                error: ErrorType::Server(error_message)
            }
        }
        other @ _ => {
            let err_msg = format!("Unexpected message {:?}", other);
            ConsumeError {
                client: client,
                error: ErrorType::Io(io::Error::new(io::ErrorKind::InvalidData, err_msg))
            }
        }
    }
}


impl <D: Debug> Stream for Consume<D> {
    type Item = Event<D>;
    type Error = ConsumeError<D>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.event_limit_reached() {
            return Ok(Async::Ready(None));
        }

        let poll_state = match self.state {
            State::RequestStart(ref mut send) => {
                let client = try_ready!(send.poll());
                let await_response = AwaitResponse::new(client, self.op_id);
                new_state(State::ReceiveStart(await_response))
            }
            State::ReceiveStart(ref mut recv) => {
                let (response, client) = try_ready!(recv.poll());
                let (batch_size, new_state) = try_ready!(response_received(self.op_id, response, client));
                self.batch_size = batch_size;
                Ok(Async::Ready(PollSuccess::NewState(new_state)))
            }
            State::ReceiveEvents(ref mut receiver) => {
                receiver.poll(self.op_id)
            }
            State::SendNextBatch(ref mut sender) => {
                let client = try_ready!(sender.poll());
                let receiver = EventReceiver(Some(client));
                Ok(Async::Ready(PollSuccess::NewState(State::ReceiveEvents(receiver))))
            }
        };

        let poll_success: PollSuccess<D> = try_ready!(poll_state);

        match poll_success {
            PollSuccess::Event(event) => {
                self.decrement_events_remaining();
                Ok(Async::Ready(Some(event)))
            }
            PollSuccess::NewState(new_state) => {
                debug!("consumer for op_id: {} transitioning from state: {:?} to {:?}", self.op_id, self.state, new_state);
                self.state = new_state;
                self.poll()
            }
        }
    }
}

impl <D: Debug> Debug for Consume<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Consume{{ namespace: '{}', total_events_remaining: {:?}, state: {:?} }}", self.namespace, self.total_events_remaining, self.state)
    }
}

enum PollSuccess<D: Debug> {
    Event(Event<D>),
    NewState(State<D>),
}

enum State<D: Debug> {
    RequestStart(SendMessage<D>),
    ReceiveStart(AwaitResponse<D>),
    ReceiveEvents(EventReceiver<D>),
    SendNextBatch(SendMessage<D>),
}

impl <D: Debug> Debug for State<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let state_desc = match *self {
            State::RequestStart(_) => "RequestStart",
            State::ReceiveStart(_) => "ReceiveStart",
            State::ReceiveEvents(_) => "ReceiveEvents",
            State::SendNextBatch(_) => "SendNextBatch",
        };
        write!(f, "{}", state_desc)
    }
}


#[derive(Debug)]
pub struct ConsumeError<D: Debug> {
    client: AsyncClient<D>,
    error: ErrorType,
}

impl <D: Debug> From<AwaitResponseError<D>> for ConsumeError<D> {
    fn from(send_err: AwaitResponseError<D>) -> Self {
        let AwaitResponseError{client, err} = send_err;
        ConsumeError {
            client: client,
            error: ErrorType::Io(err),
        }
    }
}


impl <D: Debug> From<SendError<D>> for ConsumeError<D> {
    fn from(send_err: SendError<D>) -> Self {
        let SendError{client, err} = send_err;
        ConsumeError {
            client: client,
            error: ErrorType::Io(err),
        }
    }
}


struct EventReceiver<D: Debug>(Option<AsyncClient<D>>);

impl <D: Debug> EventReceiver<D> {

    fn poll(&mut self, op_id: u32) -> PollState<D> {
        let recv_poll = {
            let client = self.0.as_mut().expect("Attempted to poll Consume after completion");
            let recv = client.recv.as_mut().expect("Client is missing receiver");
            recv.poll()
        };

        let next_message = match recv_poll {
            Ok(Async::Ready(next)) => next,
            Ok(Async::NotReady) => {
                return Ok(Async::NotReady);
            }
            Err(io_err) => {
                return Err(ConsumeError {
                    client: self.0.take().unwrap(),
                    error: ErrorType::Io(io_err)
                });
            }
        };

        match next_message {
            Some(ProtocolMessage::ReceiveEvent(event_msg)) => {
                self.convert_received(event_msg, op_id)
            }
            Some(ProtocolMessage::EndOfBatch) => {
                debug!("Received EndOfBatch for consumer with op_id: {}, requesting next batch", op_id);
                self.start_requesting_new_batch()
            }
            Some(ProtocolMessage::AwaitingEvents) => {
                debug!("Received AwaitingEvents for consumer with op_id: {}", op_id);
                Ok(Async::NotReady)
            }
            Some(other) => {
                Err(consume_error(self.0.take().unwrap(), other))
            }
            None => {
                Err(ConsumeError {
                    client: self.0.take().unwrap(),
                    error: ErrorType::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "Connection Closed"))
                })
            }
        }
    }

    fn start_requesting_new_batch(&mut self) -> PollState<D> {
        let client = self.0.take().unwrap();
        let message = ProtocolMessage::NextBatch;
        let new_state = State::SendNextBatch(SendMessage::new(client, message));
        Ok(Async::Ready(PollSuccess::NewState(new_state)))
    }

    fn convert_received(&mut self, event: RecvEvent, op_id: u32) -> PollState<D> {
        let event = event.into_owned();
        let event_id = event.id;
        let converted = {
            self.0.as_ref().unwrap().codec.convert_from_message(event)
        };

        match converted {
            Ok(event) => Ok(Async::Ready(PollSuccess::Event(event))),
            Err(codec_err) => {
                warn!("Consumer with op_id: {} error converting event {}: {:?}", op_id, event_id, codec_err);
                Err(ConsumeError{
                    client: self.0.take().unwrap(),
                    error: ErrorType::Codec(codec_err),
                })
            }
        }

    }
}
