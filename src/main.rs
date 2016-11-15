#![feature(conservative_impl_trait)]

extern crate flo;
extern crate log4rs;

#[macro_use]
extern crate clap;

#[macro_use]
extern crate nom;
/*
It's important that nom comes before log.
Both nom and log define an `error!` macro, and whichever one comes last wins.
Since we want to use the one from the log crate, that has to go last.
*/
#[macro_use]
extern crate log;
extern crate lru_time_cache;
extern crate byteorder;


#[cfg(test)]
extern crate tempdir;

#[cfg(test)]
extern crate env_logger;

pub mod event_store;
pub mod event;

mod logging;

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

}

fn parse_arg_or_exit<T: FromStr + Default>(args: &ArgMatches, arg_name: &str, default: T) -> T {
    args.value_of(arg_name)
        .map(|value| {
            match value.parse() {
                Ok(parsed) => parsed,
                Err(_) => {
                    panic!("Argument: {} is invalid", arg_name);
                }
            }
        })
        .unwrap_or(default)
}
