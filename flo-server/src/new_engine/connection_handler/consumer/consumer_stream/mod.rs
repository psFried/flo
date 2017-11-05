mod status_check;
mod notifier;

use std::io;

use futures::{Stream, Poll, Async};

use event::ActorId;
use new_engine::ConnectionId;
use new_engine::event_stream::partition::{PartitionReader, ConsumerNotifier, PersistentEvent};
use protocol::{ProtocolMessage, RecvEvent};

pub use self::notifier::{ConsumerTaskSetter, create_consumer_notifier};
pub use self::status_check::{ConsumerStatus, ConsumerStatusChecker, ConsumerStatusSetter, create_status_channel};

#[derive(Debug)]
pub struct PendingConsumer {
    pub op_id: u32,
    pub partition: ActorId,
    pub task_setter: ConsumerTaskSetter,
    pub max_events: Option<u64>,
}

pub fn prepare_consumer_start(op_id: u32, max_events: Option<u64>, connection_id: ConnectionId, partition: ActorId) -> (PendingConsumer, Box<ConsumerNotifier>) {
    let (task_setter, notifier) = create_consumer_notifier(connection_id);
    let pending = PendingConsumer {
        op_id: op_id,
        partition: partition,
        task_setter: task_setter,
        max_events: max_events,
    };
    (pending, notifier)
}


pub struct Consumer {
    connection_id: ConnectionId,
    op_id: u32,
    total_events_remaining: Option<u64>,
    batch_size: u32,
    batch_remaining: u32,

    /// whether the EndOfBatch message was sent already or not
    end_of_batch_sent: bool,

    /// actually reads events from a partition
    reader: PartitionReader,

    /// used to communicate task status back to the engine
    task_setter: ConsumerTaskSetter,

    /// used to receive consumer status from ConnectionHandler
    status_checker: ConsumerStatusChecker
}

impl Consumer {
    pub fn new(connection_id: ConnectionId,
               batch_size: u32,
               status_checker: ConsumerStatusChecker,
               reader: PartitionReader,
               pending: PendingConsumer) -> Consumer {

        let PendingConsumer {task_setter, max_events, op_id, ..} = pending;

        Consumer {
            connection_id: connection_id,
            op_id: op_id,
            total_events_remaining: max_events,
            batch_size: batch_size,
            batch_remaining: batch_size,
            reader: reader,
            task_setter: task_setter,
            status_checker: status_checker,
            end_of_batch_sent: false,
        }
    }

    fn is_done(&self) -> bool {
        self.total_events_remaining.map(|n| n == 0).unwrap_or(false)
    }

    fn await_more_events(&mut self) -> Poll<Option<ProtocolMessage>, ConsumerError> {
        trace!("Awaiting more events for connection_id: {}", self.connection_id);
        self.task_setter.await_more_events();
        Ok(Async::NotReady)
    }

    fn send_event(&mut self, event: PersistentEvent) -> Poll<Option<ProtocolMessage>, ConsumerError> {
        use event::FloEvent;

        // decrement total count and batch remaining. We've already checked to ensure that both counts are > 0
        if let Some(ref mut total) = self.total_events_remaining {
            *total -= 1;
        }
        self.batch_remaining -= 1;

        trace!("Sending event: {} to connection_id: {}", event.id(), self.connection_id);

        if self.batch_remaining == 0 {
            trace!("Batch is now exhausted for connection_id: {}", self.connection_id);
            // if we're at the end of the batch, then we need to register to be notified when the status changes
            // This call does not actually block, but just registers to be notified at a later point
            self.status_checker.await_status_change();
        }

        // TODO: Allow ProtocolMessage to work with PersistentEvents to get rid of this copy
        let message = ProtocolMessage::ReceiveEvent(RecvEvent::Owned(event.to_owned()));

        // return the event, which will get forwarded to the client Sink
        Ok(Async::Ready(Some(message)))
    }

    fn read_err(&mut self, err: io::Error) -> Poll<Option<ProtocolMessage>, ConsumerError> {
        error!("Read error for consumer: connection_id: {}, op_id: {}, err: {:?}", self.connection_id, self.op_id, err);

        // set the total remaining to 0 to make sure that all future poll calls will return None
        self.total_events_remaining = Some(0);

        Err(err.into())
    }


    fn check_status(&mut self) -> Poll<Option<StreamStatus>, ConsumerError> {
        if self.is_done() {
            return Ok(Async::Ready(None));
        }

        let batch_remaining = self.batch_remaining;

        match self.status_checker.get() {
            ConsumerStatus::NoChange => {
                if batch_remaining > 0 {
                    Ok(Async::Ready(Some(StreamStatus::Continue)))
                } else {
                    // We're at the end of a batch, so we need to wait for the status to change
                    self.status_checker.await_status_change();

                    if self.end_of_batch_sent {
                        debug!("consumer for connection_id: {} still awaiting next batch", self.connection_id);
                        Ok(Async::NotReady)
                    } else {
                        debug!("consumer for connection_id: {} sending end of batch", self.connection_id);
                        self.end_of_batch_sent = true;
                        Ok(Async::Ready(Some(StreamStatus::EndOfBatch)))
                    }
                }
            },
            ConsumerStatus::Stop => {
                debug!("Received Stop status for consumer: connection_id: {}, op_id: {}", self.connection_id, self.op_id);
                self.total_events_remaining = Some(0);
                Ok(Async::Ready(None))
            },
            ConsumerStatus::NextBatch => {
                debug!("Resetting batch counter for consumer: connection_id: {}, op_id: {}, batch_size: {}, batch_remaining: {}",
                        self.connection_id, self.op_id, self.batch_size, self.batch_remaining);

                self.batch_remaining = self.batch_size;
                self.end_of_batch_sent = false;
                Ok(Async::Ready(Some(StreamStatus::Continue)))
            },
        }
    }

    fn next_matching_result(&mut self) -> Poll<Option<ProtocolMessage>, ConsumerError> {
        let result = self.reader.next_matching();
        match result {
            None => self.await_more_events(),
            Some(Ok(event)) => self.send_event(event),
            Some(Err(io_err)) => self.read_err(io_err),
        }
    }
}

#[derive(Debug)]
enum StreamStatus {
    EndOfBatch,
    Continue
}


impl Stream for Consumer {
    type Item = ProtocolMessage;
    type Error = ConsumerError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let stream_status = try_ready!(self.check_status());

        match stream_status {
            None => {
                Ok(Async::Ready(None))
            }
            Some(StreamStatus::EndOfBatch) => {
                Ok(Async::Ready(Some(ProtocolMessage::EndOfBatch)))
            }
            Some(StreamStatus::Continue) => {
                self.next_matching_result()
            }
        }
    }
}

use futures::sync::mpsc::SendError;

#[derive(Debug)]
pub enum ConsumerError {
    Send(SendError<ProtocolMessage>),
    Read(io::Error)
}

impl From<io::Error> for ConsumerError {
    fn from(err: io::Error) -> Self {
        ConsumerError::Read(err)
    }
}

impl From<SendError<ProtocolMessage>> for ConsumerError {
    fn from(err: SendError<ProtocolMessage>) -> Self {
        ConsumerError::Send(err)
    }
}
