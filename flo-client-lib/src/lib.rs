#[cfg(feature = "serde-client")]
extern crate serde;

#[cfg(feature = "serde-json-codec")]
extern crate serde_json;

extern crate flo_event as event;
extern crate flo_protocol as protocol;

#[macro_use]
extern crate log;

#[macro_use]
extern crate futures;
extern crate tokio_core;

pub mod codec;
pub mod sync;
pub mod async;

pub use protocol::{ErrorKind, ErrorMessage};
pub use event::{
    time,
    FloEventId,
    VersionVector,
    ActorId,
    EventCounter,
    Timestamp,
    OwnedFloEvent
};

pub const ALL_EVENTS_GLOB: &'static str = "/**/*";

/// An event that can be received by a `Consumer`. It is parameterized on the type of the body, which will be determined
/// by the `EventCodec` used.
#[derive(Debug, PartialEq, Clone)]
pub struct Event<T> {
    pub id: FloEventId,
    pub parent_id: Option<FloEventId>,
    pub timestamp: Timestamp,
    pub namespace: String,
    pub data: T
}
