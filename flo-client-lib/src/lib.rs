pub extern crate flo_event as event;
extern crate flo_protocol as protocol;

#[macro_use]
extern crate log;

#[cfg(feature = "serde-client")]
extern crate serde;

pub mod sync;

pub use protocol::{ErrorKind, ErrorMessage};
pub use event::{time, FloEventId, FloEvent, OwnedFloEvent, ActorId, EventCounter, Timestamp};
pub use sync::{
    SyncConnection,
    ConsumerAction,
    ConsumerContext,
    FloConsumer,
    ConsumerOptions,
    ClientError
};
