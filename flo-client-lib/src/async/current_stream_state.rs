

use std::fmt::Debug;

use event::{ActorId, EventCounter};

#[derive(Debug, PartialEq, Clone)]
pub struct PartitionState {
    pub partition_num: ActorId,
    pub head: EventCounter,
    pub writable: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CurrentStreamState {
    pub name: String,
    pub partitions: Vec<PartitionState>,
}

impl From<::protocol::PartitionStatus> for PartitionState {
    fn from(status: ::protocol::PartitionStatus) -> Self {
        let ::protocol::PartitionStatus { partition_num, head, primary } = status;

        PartitionState {
            partition_num: partition_num,
            head: head,
            writable: primary,
        }
    }
}

impl From<::protocol::EventStreamStatus> for CurrentStreamState {
    fn from(status: ::protocol::EventStreamStatus) -> Self {
        let ::protocol::EventStreamStatus { name, partitions, .. } = status;

        let part_statuses = partitions.into_iter().map(|p| p.into()).collect::<Vec<PartitionState>>();

        CurrentStreamState {
            name: name,
            partitions: part_statuses
        }
    }
}
