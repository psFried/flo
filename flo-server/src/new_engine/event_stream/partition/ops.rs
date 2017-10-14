
use std::io;
use std::fmt::{self, Debug};
use std::time::Instant;

use futures::sync::oneshot;

use new_engine::event_stream::partition::{EventFilter, PartitionReader};
use new_engine::ConnectionId;
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

pub struct ConsumeOperation {
    pub client_sender: oneshot::Sender<PartitionReader>,
    pub filter: EventFilter,
    pub start_exclusive: EventCounter,
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
}


#[derive(Debug)]
pub struct Operation {
    pub connection_id: ConnectionId,
    pub client_message_recv_time: Instant,
    pub op_type: OpType,
}

impl Operation {
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
}

