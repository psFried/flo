extern crate flo;
extern crate log4rs;

#[macro_use]
extern crate log;
extern crate rotor;
extern crate rotor_http;
extern crate netbuf;
extern crate serde_json;
extern crate queryst;
extern crate lru_time_cache;

#[cfg(test)]
extern crate httparse;

#[cfg(test)]
extern crate tempdir;


pub mod server;
pub mod context;
pub mod event_store;
pub mod event;

mod logging;

#[cfg(test)]
mod test_utils;


use logging::init_logging;

fn main() {
    init_logging();
    server::start_server();
}
