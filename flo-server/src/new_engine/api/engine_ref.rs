use protocol::{ProduceEvent, ProtocolMessage};
use new_engine::api::{ConnectionId, PartitionOperation, EngineSender};
use event::ActorId;

use std::time::Instant;
use std::collections::HashMap;
use std::sync::Mutex;


#[derive(Clone)]
pub struct PartitionRef {
    event_stream_name: String,
    partion_num: ActorId,
    sender: EngineSender,
}

#[derive(Clone)]
pub struct EventStreamRef {
    name: String,
    partitions: Vec<PartitionRef>,
}

pub enum PartitionSendError {
    OutOfBounds(PartitionOperation),
    ChannelError(PartitionOperation)
}

pub type EventStreamSendResult = Result<(), PartitionSendError>;

impl EventStreamRef {
    pub fn get_partition_count(&self) -> ActorId {
        self.partitions.len() as ActorId
    }

    pub fn send_to_partition(&mut self, partition: ActorId, op: PartitionOperation) -> EventStreamSendResult {
        match self.partitions.get_mut(partition as usize) {
            Some(ref mut partition) => partition.sender.send(op).map_err(|e| PartitionSendError::ChannelError(e.0)),
            None => Err(PartitionSendError::OutOfBounds(op))
        }
    }
}

pub struct EngineRef {
    current_connection_id: ::std::sync::atomic::AtomicUsize,
    event_streams: Mutex<HashMap<String, EventStreamRef>>
}

#[derive(Debug)]
pub enum ConnectError {
    InitFailed(::std::io::Error),
    NoStream,
}

impl EngineRef {
    pub fn connect_client(&self, stream_name: &str) -> Result<EventStreamRef, ConnectError> {
        let streams = self.event_streams.lock().unwrap();
        if let Some(stream) = streams.get(stream_name).map(|s| s.clone()) {
            Ok(stream)
        } else {
            Err(ConnectError::NoStream)
        }
    }
}
