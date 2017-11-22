
use engine::event_stream::partition::{PartitionRef};
use engine::event_stream::EventStreamRef;



#[derive(Clone, Debug)]
pub struct SystemStreamRef {
    inner: PartitionRef,
}

impl SystemStreamRef {

    pub fn new(partition_ref: PartitionRef) -> SystemStreamRef {
        SystemStreamRef {
            inner: partition_ref,
        }
    }

    pub fn to_event_stream(&self) -> EventStreamRef {
        let name = self.inner.event_stream_name().to_owned();
        let partition_ref = vec![self.inner.clone()];
        EventStreamRef::new(name, partition_ref)
    }
}






