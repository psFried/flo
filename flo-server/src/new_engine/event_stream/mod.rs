pub mod partition;

use std::time::Duration;
use std::path::PathBuf;
use std::io;

use event::ActorId;
use self::partition::{PartitionRef, PartitionSendError, PartitionSendResult, ProduceOperation, Operation};


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




pub fn init_event_stream(storage_dir: PathBuf, options: EventStreamOptions) -> Result<EventStreamRef, io::Error> {
    unimplemented!()
}



#[derive(Clone)]
pub struct EventStreamRef {
    name: String,
    partitions: Vec<PartitionRef>,
}

impl EventStreamRef {
    pub fn get_partition_count(&self) -> ActorId {
        self.partitions.len() as ActorId
    }

    pub fn get_partition(&mut self, partition: ActorId) -> Option<&mut PartitionRef> {
        self.partitions.get_mut(partition as usize)
    }
}




