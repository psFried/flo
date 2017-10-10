

use std::fmt::Debug;

use event::{ActorId, EventCounter};

#[derive(Debug, PartialEq, Clone)]
pub struct PartitionStatus {
    partition_num: ActorId,
    head: EventCounter,
    writable: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct EventStreamStatus {
    name: String,
    partitions: Vec<PartitionStatus>,
}

impl From<::protocol::PartitionStatus> for PartitionStatus {
    fn from(status: ::protocol::PartitionStatus) -> Self {
        let ::protocol::PartitionStatus { partition_num, head, primary } = status;

        PartitionStatus {
            partition_num: partition_num,
            head: head,
            writable: primary,
        }
    }
}

impl From<::protocol::EventStreamStatus> for EventStreamStatus {
    fn from(status: ::protocol::EventStreamStatus) -> Self {
        let ::protocol::EventStreamStatus { name, partitions, .. } = status;

        let part_statuses = partitions.into_iter().map(|p| p.into()).collect::<Vec<PartitionStatus>>();

        EventStreamStatus {
            name: name,
            partitions: part_statuses
        }
    }
}
