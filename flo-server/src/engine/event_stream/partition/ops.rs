
use std::io;
use std::fmt::{self, Debug};
use std::time::Instant;

use futures::sync::oneshot;

use engine::event_stream::partition::{EventFilter, PartitionReader};
use engine::ConnectionId;
use protocol::ProduceEvent;
use event::{FloEventId, EventCounter};

pub type ProduceResult = Result<FloEventId, io::Error>;
pub type ProduceResponder = oneshot::Sender<ProduceResult>;
pub type ProduceResponseReceiver = oneshot::Receiver<ProduceResult>;

pub struct ProduceOperation {
    pub client: oneshot::Sender<io::Result<FloEventId>>,
    pub op_id: u32,
    pub events: Vec<ProduceEvent>,
}


impl Debug for ProduceOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ProduceOperation {{ op_id: {}, events: {:?} }}", self.op_id, self.events)
    }
}

pub type ConsumeResponder = oneshot::Sender<PartitionReader>;
pub type ConsumeResponseReceiver = oneshot::Receiver<PartitionReader>;

pub trait ConsumerNotifier: Send {
    /// Notify the consumer that an event is ready to be read.
    /// The impl just calls `notify()` on the `futures::task::Task` associated with the consumer
    fn notify(&self);
    /// returns `false` if the consumer is finished and will never again want to be notified about future events. Otherwise, `true`
    fn is_active(&self) -> bool;
    /// returns the `ConnectionId` of this consumer
    fn connection_id(&self) -> ConnectionId;
}

pub struct ConsumeOperation {
    pub client_sender: oneshot::Sender<PartitionReader>,
    pub filter: EventFilter,
    pub start_exclusive: EventCounter,
    pub notifier: Box<ConsumerNotifier>,
}

impl Debug for ConsumeOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConsumeOperation {{ filter: {:?}, start_exclusive: {} }}", self.filter, self.start_exclusive)
    }
}

#[derive(Debug)]
pub enum OpType {
    Produce(ProduceOperation),
    Consume(ConsumeOperation),
    StopConsumer,
    Tick,
}


#[derive(Debug)]
pub struct Operation {
    pub connection_id: ConnectionId,
    pub client_message_recv_time: Instant,
    pub op_type: OpType,
}

impl Operation {
    pub fn consume(connection_id: ConnectionId, notifier: Box<ConsumerNotifier>, filter: EventFilter, start_exclusive: EventCounter) -> (Operation, ConsumeResponseReceiver) {
        let (tx, rx) = oneshot::channel();
        let consume = ConsumeOperation {
            client_sender: tx,
            filter: filter,
            start_exclusive: start_exclusive,
            notifier: notifier,
        };
        let op = Operation {
            connection_id: connection_id,
            client_message_recv_time: Instant::now(),
            op_type: OpType::Consume(consume)
        };
        (op, rx)
    }

    pub fn stop_consumer(connection_id: ConnectionId) -> Operation {
        Operation {
            connection_id: connection_id,
            client_message_recv_time: Instant::now(),
            op_type: OpType::StopConsumer
        }
    }

    pub fn produce(connection_id: ConnectionId, op_id: u32, events: Vec<ProduceEvent>) -> (Operation, ProduceResponseReceiver) {
        let (tx, rx) = oneshot::channel();
        let produce = ProduceOperation {
            client: tx,
            op_id: op_id,
            events: events
        };
        let op = Operation {
            connection_id: connection_id,
            client_message_recv_time: Instant::now(),
            op_type: OpType::Produce(produce),
        };
        (op, rx)
    }

    pub fn tick() -> Operation {
        Operation {
            connection_id: 0,
            client_message_recv_time: Instant::now(),
            op_type: OpType::Tick,
        }
    }
}

