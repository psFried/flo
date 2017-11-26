
use super::FloInstanceId;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SystemReplicationPlan {
    pub streams: Vec<StreamReplicationPlan>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StreamReplicationPlan {
    pub name: String,
    pub partitions: Vec<PartitionReplicationPlan>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PartitionReplicationPlan {
    pub primary: FloInstanceId,
    // TODO: Add ability to limit replication to only a subset of secondary nodes to reduce network traffic
}
