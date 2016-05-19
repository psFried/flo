extern crate flo;
extern crate log4rs;

#[macro_use]
extern crate clap;

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
use clap::{App, Arg, ArgMatches};
use std::str::FromStr;
use std::path::PathBuf;

const FLO_VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    init_logging();

    let args = App::new("flo")
            .version(FLO_VERSION)
            .arg(Arg::with_name("port")
                    .short("p")
                    .long("port")
                    .value_name("PORT")
                    .help("port that the server should listen on")
                    .default_value("3000"))
            .arg(Arg::with_name("data-dir")
                    .short("d")
                    .long("data-dir")
                    .value_name("DIR")
                    .help("The directory to be used for storage")
                    .default_value("."))
            .get_matches();

    let port = parse_arg_or_exit(&args, "port", 3000u16);
    let data_dir = PathBuf::from(args.value_of("data-dir").unwrap_or("."));

    server::start_server(server::ServerOptions{
        port: port,
        storage_dir: data_dir,
    });
}

fn parse_arg_or_exit<T: FromStr + Default>(args: &ArgMatches, arg_name: &str, default: T) -> T {
    use std::process;

    args.value_of(arg_name).map(|value| {
        match value.parse() {
            Ok(parsed) => parsed,
            Err(e) => {
                panic!("Argument: {} is invalid", arg_name);
            }
        }
    }).unwrap_or(default)
}
