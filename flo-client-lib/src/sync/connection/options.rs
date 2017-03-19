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
    pub fn simple<S: Into<String>>(namespace: S, start_position: FloEventId, max_events: u64) -> ConsumerOptions {
        let mut vv = VersionVector::new();
        vv.update_if_greater(start_position);
        ConsumerOptions::new(namespace, vv, max_events)
    }

    pub fn from_beginning<S: Into<String>>(namespace: S, max_events: u64) -> ConsumerOptions {
        ConsumerOptions::new(namespace, VersionVector::new(), max_events)
    }

    pub fn new<S: Into<String>>(namespace: S, start_position: VersionVector, max_events: u64) -> ConsumerOptions {
        ConsumerOptions {
            namespace: namespace.into(),
            version_vector: start_position,
            max_events: max_events,
        }
    }
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        ConsumerOptions::new(::ALL_EVENTS_GLOB, VersionVector::new(), ::std::u64::MAX)
    }
}


