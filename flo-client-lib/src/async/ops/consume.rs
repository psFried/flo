
use std::fmt::{self, Debug};
use std::io;

use futures::{Future, Async, Poll, Stream};

use event::VersionVector;
use protocol::{ProtocolMessage, NewConsumerStart, CONSUME_UNLIMITED, RecvEvent};
use async::{AsyncConnection, ErrorType};
use async::ops::{SendMessage, SendError, AwaitResponse, AwaitResponseError};
use ::Event;


pub struct Consume<D: Debug> {
    op_id: u32,
    batch_size: u32,
    namespace: String,
    await_new_events: bool,
    total_events_remaining: Option<u64>,
    state: State<D>,
}

type PollState<D> = Poll<PollSuccess<D>, ConsumeError<D>>;

impl <D: Debug> Consume<D> {

    pub fn new(mut connection: AsyncConnection<D>, namespace: String, version_vec: &VersionVector, event_limit: Option<u64>, await_new: bool) -> Consume<D> {
        let op_id = connection.next_op_id();
        let consumer_start = NewConsumerStart {
            op_id: op_id,
            version_vector: version_vec.snapshot(),
            max_events: event_limit.unwrap_or(CONSUME_UNLIMITED),
            namespace: namespace.clone(),
        };
        let message = ProtocolMessage::NewStartConsuming(consumer_start);
        let initial_state = State::RequestStart(SendMessage::new(connection, message));

        Consume {
            op_id: op_id,
            batch_size: 0,
            namespace: namespace,
            await_new_events: await_new,
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

fn response_received<D: Debug>(op_id: u32, response: ProtocolMessage, connection: AsyncConnection<D>) -> Result<Async<(u32, State<D>)>, ConsumeError<D>> {
    match response {
        ProtocolMessage::CursorCreated(info) => {
            debug!("Consumer with op_id: {} received CursorCreated: {:?}", op_id, info);
            let new_state = State::ReceiveEvents(EventReceiver(Some(connection)));
            Ok(Async::Ready((info.batch_size, new_state)))
        }
        other @ _ => {
            warn!("consumer with op_id: {} received error response: {:?}", op_id, other);
            Err(consume_error(connection, other))
        }
    }
}

fn new_state<D: Debug>(state: State<D>) -> PollState<D> {
    Ok(Async::Ready(PollSuccess::NewState(state)))
}

fn consume_error<D: Debug>(connection: AsyncConnection<D>, message: ProtocolMessage) -> ConsumeError<D> {
    match message {
        ProtocolMessage::Error(error_message) => {
            ConsumeError {
                connection: connection,
                error: ErrorType::Server(error_message)
            }
        }
        other @ _ => {
            let err_msg = format!("Unexpected message {:?}", other);
            ConsumeError {
                connection: connection,
                error: ErrorType::Io(io::Error::new(io::ErrorKind::InvalidData, err_msg))
            }
        }
    }
}

impl <D: Debug> Into<AsyncConnection<D>> for Consume<D> {
    fn into(self) -> AsyncConnection<D> {
        match self.state {
            State::RequestStart(send) => send.into(),
            State::ReceiveStart(recv) => recv.into(),
            State::ReceiveEvents(recve) => recve.into(),
            State::SendNextBatch(next) => next.into(),
        }
    }
}

impl <D: Debug> Stream for Consume<D> {
    type Item = Event<D>;
    type Error = ConsumeError<D>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.event_limit_reached() {
            debug!("Consumer for op_id: {} is finished because event limit was reached", self.op_id);
            return Ok(Async::Ready(None));
        }

        let poll_state = match self.state {
            State::RequestStart(ref mut send) => {
                let connection = try_ready!(send.poll());
                let await_response = AwaitResponse::new(connection, self.op_id);
                new_state(State::ReceiveStart(await_response))
            }
            State::ReceiveStart(ref mut recv) => {
                let (response, connection) = try_ready!(recv.poll());
                let (batch_size, new_state) = try_ready!(response_received(self.op_id, response, connection));
                self.batch_size = batch_size;
                Ok(Async::Ready(PollSuccess::NewState(new_state)))
            }
            State::ReceiveEvents(ref mut receiver) => {
                receiver.poll(self.op_id)
            }
            State::SendNextBatch(ref mut sender) => {
                let connection = try_ready!(sender.poll());
                let receiver = EventReceiver(Some(connection));
                Ok(Async::Ready(PollSuccess::NewState(State::ReceiveEvents(receiver))))
            }
        };

        let poll_success: PollSuccess<D> = try_ready!(poll_state);

        match poll_success {
            PollSuccess::AwaitReceived if self.await_new_events => {
                // just poll again to make sure we're registered to get notified when the next event is ready
                self.poll()
            }
            PollSuccess::AwaitReceived => {
                debug!("Consumer for op_id: {} is finished because AwaitingEvents was received and await_new=false", self.op_id);
                Ok(Async::Ready(None))
            }
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
    AwaitReceived,
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
    connection: AsyncConnection<D>,
    error: ErrorType,
}

impl <D: Debug> From<AwaitResponseError<D>> for ConsumeError<D> {
    fn from(send_err: AwaitResponseError<D>) -> Self {
        let AwaitResponseError{connection, err} = send_err;
        ConsumeError {
            connection: connection,
            error: ErrorType::Io(err),
        }
    }
}


impl <D: Debug> From<SendError<D>> for ConsumeError<D> {
    fn from(send_err: SendError<D>) -> Self {
        let SendError{connection, err} = send_err;
        ConsumeError {
            connection: connection,
            error: ErrorType::Io(err),
        }
    }
}


struct EventReceiver<D: Debug>(Option<AsyncConnection<D>>);

impl <D: Debug> EventReceiver<D> {

    fn poll(&mut self, op_id: u32) -> PollState<D> {
        let recv_poll = {
            let connection = self.0.as_mut().expect("Attempted to poll Consume after completion");
            let recv = connection.inner.recv.as_mut().expect("Client is missing receiver");
            recv.poll()
        };

        let next_message = match recv_poll {
            Ok(Async::Ready(next)) => next,
            Ok(Async::NotReady) => {
                return Ok(Async::NotReady);
            }
            Err(io_err) => {
                return Err(ConsumeError {
                    connection: self.0.take().unwrap(),
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
                Ok(Async::Ready(PollSuccess::AwaitReceived))
            }
            Some(other) => {
                Err(consume_error(self.0.take().unwrap(), other))
            }
            None => {
                Err(ConsumeError {
                    connection: self.0.take().unwrap(),
                    error: ErrorType::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "Connection Closed"))
                })
            }
        }
    }

    fn start_requesting_new_batch(&mut self) -> PollState<D> {
        let connection = self.0.take().unwrap();
        let message = ProtocolMessage::NextBatch;
        let new_state = State::SendNextBatch(SendMessage::new(connection, message));
        Ok(Async::Ready(PollSuccess::NewState(new_state)))
    }

    fn convert_received(&mut self, event: RecvEvent, op_id: u32) -> PollState<D> {
        let event = event.into_owned();
        let event_id = event.id;
        let converted = {
            self.0.as_ref().unwrap().inner.codec.convert_from_message(event)
        };

        match converted {
            Ok(event) => Ok(Async::Ready(PollSuccess::Event(event))),
            Err(codec_err) => {
                warn!("Consumer with op_id: {} error converting event {}: {:?}", op_id, event_id, codec_err);
                Err(ConsumeError{
                    connection: self.0.take().unwrap(),
                    error: ErrorType::Codec(codec_err),
                })
            }
        }

    }
}

impl <D: Debug> Into<AsyncConnection<D>> for EventReceiver<D> {
    fn into(mut self) -> AsyncConnection<D> {
        self.0.take().expect("EventReceiver has already been completed")
    }
}
