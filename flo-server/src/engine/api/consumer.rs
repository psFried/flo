use super::{NamespaceGlob, ConnectionId};
use event::{FloEventId, ActorId, OwnedFloEvent, VersionVector};

#[derive(Debug, Clone, PartialEq)]
pub enum ConsumerFilter {
    Namespace(NamespaceGlob),
    ActorId(Vec<ActorId>),
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConsumerState {
    pub version_vector: VersionVector,
    pub filter: ConsumerFilter,
    pub connection_id: ConnectionId,
    pub batch_remaining: u64,
    pub batch_size: u64,
}

impl ConsumerState {

    pub fn new(connection_id: ConnectionId, version_vec: VersionVector, filter: ConsumerFilter, batch_size: u64) -> ConsumerState {
        ConsumerState {
            connection_id: connection_id,
            version_vector: version_vec,
            filter: filter,
            batch_remaining: batch_size,
            batch_size: batch_size,
        }
    }

    pub fn from_namespace(connection_id: ConnectionId, version_vec: VersionVector, namespace: NamespaceGlob, batch_size: u64) -> ConsumerState {
        ConsumerState::new(connection_id, version_vec, ConsumerFilter::Namespace(namespace), batch_size)
    }

    pub fn for_peer(connection_id: ConnectionId, version_vec: VersionVector, batch_size: u64) -> ConsumerState {
        ConsumerState::new(connection_id, version_vec, ConsumerFilter::All, batch_size)
    }

    pub fn should_send_event(&self, event: &OwnedFloEvent) -> bool {
        unimplemented!()
    }

    pub fn event_sent(&mut self, id: FloEventId) {

        unimplemented!()
    }

    pub fn is_batch_exhausted(&self) -> bool {
        self.batch_remaining > 0
    }

    pub fn start_new_batch(&mut self) {
        self.batch_remaining = self.batch_size;
    }
}


