#![feature(conservative_impl_trait)]

#[macro_use]
extern crate nom;

/*
It's important that nom comes before log.
Both nom and log define an `error!` macro, and whichever one comes last wins.
Since we want to use the one from the log crate, that has to go last.
*/
#[macro_use]
extern crate log;

#[cfg(test)]
extern crate env_logger;

pub mod event;
pub mod protocol;
