#![feature(conservative_impl_trait)]

extern crate flo_event as event;
extern crate flo_protocol as protocol;

#[macro_use]
extern crate log;

extern crate memmap;
extern crate clap;
extern crate log4rs;
extern crate num_cpus;
extern crate byteorder;
extern crate tokio_core;
extern crate futures;
extern crate chrono;
extern crate glob;

#[cfg(test)]
extern crate env_logger;
#[cfg(test)]
extern crate tempdir;


pub mod logging;
pub mod server;
pub mod engine;
pub mod embedded;
pub mod new_engine;
pub mod event_loops;
mod channels;
mod atomics;
