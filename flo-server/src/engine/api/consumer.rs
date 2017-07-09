use super::{NamespaceGlob, ConnectionId};
use event::{FloEventId, ActorId, OwnedFloEvent, VersionVector};

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)] // ActorId and All aren't used at the moment, but they will be very soon
pub enum ConsumerFilter {
    Namespace(NamespaceGlob),
    ActorId(Vec<ActorId>),
    All,
}

impl ConsumerFilter {

    pub fn apply(&self, event: &OwnedFloEvent) -> bool {
        match self {
            &ConsumerFilter::Namespace(ref glob) => {
                glob.matches(&event.namespace)
            }
            &ConsumerFilter::ActorId(ref actors) => {
                actors.contains(&event.id.actor)
            }
            &ConsumerFilter::All => true
        }
    }
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

    pub fn should_send_event(&self, event: &OwnedFloEvent) -> bool {
        self.batch_remaining > 0 && !self.version_vector.contains(event.id) && self.filter.apply(event)
    }

    pub fn event_sent(&mut self, id: FloEventId) {
        self.batch_remaining = self.batch_remaining.saturating_sub(1);
        trace!("event_sent, batch_remaining: {}", self.batch_remaining);
        self.version_vector.update_if_greater(id);
    }

    pub fn is_batch_exhausted(&self) -> bool {
        self.batch_remaining == 0
    }

    pub fn start_new_batch(&mut self) {
        self.batch_remaining = self.batch_size;
    }
}


