pub extern crate flo_event as event;
extern crate flo_protocol as protocol;

#[macro_use]
extern crate log;

pub mod client;

pub use protocol::{ErrorKind, ErrorMessage};
pub use event::{time, FloEventId, FloEvent, OwnedFloEvent, ActorId, EventCounter, Timestamp};
pub use client::sync::{SyncConnection, ConsumerAction, ConsumerContext, FloConsumer};
pub use client::{ConsumerOptions, ClientError};
