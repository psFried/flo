#![feature(conservative_impl_trait)]

extern crate flo;

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

extern crate log4rs;

#[cfg(test)]
extern crate env_logger;

mod logging;

use logging::init_logging;
use clap::{App, Arg, ArgMatches};
use std::str::FromStr;
use std::path::PathBuf;
use flo::server::{self, ServerOptions, MemoryLimit, MemoryUnit};

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
                   .arg(Arg::with_name("default-namespace")
                            .long("default-namespace")
                            .value_name("ns")
                            .help("Name of the default namespace")
                            .default_value("default"))
                   .arg(Arg::with_name("max-events")
                            .long("max-events")
                            .value_name("max")
                            .help("Maximum number of events to keep, if left unspecified, defaults to max u32 or u64 depending on architecture"))
                    .arg(Arg::with_name("max-cached-events")
                            .long("max-cached-events")
                            .value_name("max")
                            .help("Maximum number of events to cache in memory, if left unspecified, defaults to max u32 or u64 depending on architecture"))
                .arg(Arg::with_name("max-cache-memory")
                        .short("M")
                        .long("max-cache-memory")
                        .value_name("megabytes")
                        .help("Maximum amount of memory in megabytes to use for the event cache"))
                   .get_matches();

    let port = parse_arg_or_exit(&args, "port", 3000u16);
    let data_dir = PathBuf::from(args.value_of("data-dir").unwrap_or("."));
    let max_events = parse_arg_or_exit(&args, "max-events", ::std::usize::MAX);
    let default_ns = args.value_of("default-namespace").map(|value| value.to_owned()).expect("Must have a value for 'default-namespace' argument");
    let max_cached_events = parse_arg_or_exit(&args, "max-cached-events", ::std::usize::MAX);
    let max_cache_memory = get_max_cache_mem_amount(&args);
    let server_options = ServerOptions {
        default_namespace: default_ns,
        max_events: max_events,
        port: port,
        data_dir: data_dir,
        max_cached_events: ::std::usize::MAX,
        max_cache_memory: max_cache_memory
    };
    server::run(server_options);
    info!("Shutdown server");
}

fn get_max_cache_mem_amount(args: &ArgMatches) -> MemoryLimit {
    let mb = parse_arg_or_exit(args, "max-cache-memory", 512usize);
    MemoryLimit::new(mb, MemoryUnit::Megabyte)
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
