use event::{FloEventId, VersionVector};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

#[derive(Debug, PartialEq, Clone)]
pub struct ConsumerOptions {
    pub namespace: String,
    pub version_vector: VersionVector,
    pub max_events: u64,
}

impl ConsumerOptions {
    pub fn simple<S: ToString>(namespace: S, start_position: VersionVector, max_events: u64) -> ConsumerOptions {
        ConsumerOptions {
            namespace: namespace.to_string(),
            version_vector: start_position,
            max_events: max_events,
        }
    }
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        ConsumerOptions::simple(::ALL_EVENTS_GLOB, VersionVector::new(), ::std::u64::MAX)
    }
}


