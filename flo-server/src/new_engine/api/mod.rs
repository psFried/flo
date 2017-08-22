mod namespace;
mod engine_ref;

use std::time::{Instant, Duration};
use std::default::Default;

use protocol::{ProduceEvent, ProtocolMessage};
use event::{EventCounter, ActorId};

pub use self::namespace::NamespaceGlob;
pub use self::engine_ref::{EngineRef, EventStreamRef, PartitionRef, PartitionSendError};


pub type ConnectionId = u64;

pub type ClientSender = ::futures::sync::mpsc::UnboundedSender<ProtocolMessage>;
pub type ClientReceiver = ::futures::sync::mpsc::UnboundedReceiver<ProtocolMessage>;
pub type EngineSender = ::std::sync::mpsc::Sender<PartitionOperation>;
pub type EngineReceiver = ::std::sync::mpsc::Receiver<PartitionOperation>;

#[derive(Debug, PartialEq)]
pub struct EventStreamOptions {
    pub name: String,
    pub num_partitions: u16,
    pub event_retention: Duration,
    pub max_segment_duration: Duration,
    pub segment_max_size_bytes: u64,
}


impl Default for EventStreamOptions {
    fn default() -> Self {
        EventStreamOptions {
            name: "default".to_owned(),
            num_partitions: 1,
            event_retention: Duration::from_secs(60 * 60 * 24 * 30), // 30 days
            max_segment_duration: Duration::from_secs(60 * 60 * 24), // 24 hours
            segment_max_size_bytes: 1024 * 1024 * 1024,                    // 1GB
        }
    }
}



pub struct ClientMessageSender {
    connection_id: ConnectionId,
    sender: ClientSender,
}

pub struct StartConsuming {
    client: ClientMessageSender,
    namespace: NamespaceGlob,
    batch_size: u32,
    limit: u64,
    start_exclusive: EventCounter
}

pub struct ProduceOperation {
    client: ClientMessageSender,
    op_id: u64,
    events: Vec<ProduceEvent>,
}

pub enum PartitionMessage {
    ConsumerStart(StartConsuming),
    NextBatch(ConnectionId),
    Produce(ProduceOperation),
}

pub struct PartitionOperation {
    client_message_recv_time: Instant,
    message: PartitionMessage,
}
