
#[macro_use]
extern crate nom;

/*
It's important that nom comes before log.
Both nom and log define an `error!` macro, and whichever one comes last wins.
Since we want to use the one from the log crate, that has to go last.
*/
#[macro_use]
extern crate log;

extern crate byteorder;
extern crate chrono;
extern crate num_cpus;
extern crate tokio_core;
extern crate futures;
extern crate glob;

#[cfg(test)]
extern crate env_logger;
#[cfg(test)]
extern crate tempdir;

pub mod client;
pub mod protocol;
pub mod time;
pub mod serializer;
pub mod error;
pub mod event;

pub use event::{FloEventId, FloEvent, OwnedFloEvent, ActorId, EventCounter, Timestamp};
pub use client::sync::{SyncConnection, ConsumerAction};
