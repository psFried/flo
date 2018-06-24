extern crate flo_server;
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


pub use flo_server::*;

use chrono::Duration;
use logging::{init_logging, LogLevelOption, LogFileOption};
use clap::{App, Arg, ArgMatches};
use std::str::FromStr;
use std::path::{PathBuf, Path};
use server::{ServerOptions, MemoryLimit, MemoryUnit};
use std::net::{SocketAddr, ToSocketAddrs};

const FLO_VERSION: &'static str = env!("CARGO_PKG_VERSION");
const MAX_SEGMENT_PERIOD_HOURS: i64 = 24;

fn app_args() -> App<'static, 'static> {
    App::new("flo")
            .version(FLO_VERSION)
            .arg(Arg::with_name("log-level")
                    .short("-L")
                    .long("log")
                    .takes_value(true)
                    .value_name("module=level")
                    .multiple(true)
                    .help("Sets the log level for a module. Argument should be in the format module::sub-module=<level> where level is one of trace, debug, info, warn"))
            .arg(Arg::with_name("log-dest")
                    .short("o")
                    .long("log-dest")
                    .value_name("path")
                    .help("Path of a file to write logs to. Default is to log to stdout if unspecified"))
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
            .arg(Arg::with_name("event-retention-days")
                    .long("event-retention-days")
                    .value_name("days")
                    .help("The minimum number of days to retain events, given as an integer. Fractional days are not supported. If unspecified, then events will never expire"))
            .arg(Arg::with_name("eviction-period")
                    .long("eviction-period")
                    .value_name("hours")
                    .help("The number of hours to go between checks for purging old events. If unspecified, then defaults to the lesser of one sixth of the total retention time or 24 hours"))
            .arg(Arg::with_name("max-cache-memory")
                    .short("M")
                    .long("max-cache-memory")
                    .value_name("megabytes")
                    .default_value("512")
                    .help("Maximum amount of memory in megabytes to use for the event cache"))
            .arg(Arg::with_name("join-cluster-address")
                    .requires("server-addr")
                    .long("peer-addr")
                    .short("P")
                    .multiple(true)
                    .value_name("HOST:PORT")
                    .help("address of another Flo instance to join a cluster; this argument may be supplied multiple times"))
            .arg(Arg::with_name("server-addr")
                    .requires("join-cluster-address")
                    .long("server-addr")
                    .short("A")
                    .takes_value(true)
                    .help("The socket address that this server is reachable at. Will be removed once cluster support doesn't suck so bad"))
            .arg(Arg::with_name("election-timeout")
                    .long("election-timeout")
                    .takes_value(true)
                    .help("Trigger an election after this number of milliseconds. Defaults to a random number between 150-300"))
            .arg(Arg::with_name("heartbeat-interval")
                    .long("heartbeat-interval")
                    .takes_value(true)
                    .help("Number of milliseconds to go in between sending heartbeats. Defaults to 1/3 of the election timeout"))
            .arg(Arg::with_name("max-io-threads")
                    .long("max-io-threads")
                    .takes_value(true)
                    .help("The maximum number of threads to spawn for handling client connections. The actual number of threads used may be less"))
}

fn main() {
    let args = app_args().get_matches();

    let log_levels = get_log_level_options(&args);
    let log_dest = get_log_file_option(&args);
    init_logging(log_dest, log_levels);

    let port = parse_arg_or_exit(&args, "port", 3000u16);
    let data_dir = PathBuf::from(args.value_of("data-dir").unwrap_or("."));
    let max_cache_memory = get_max_cache_mem_amount(&args);
    let cluster_addresses = get_cluster_addresses(&args);
    let this_address = args.value_of("server-addr").map(|addr_string| {
        SocketAddr::from_str(addr_string).map_err(|err| {
            format!("Cannot parse server-addr argument: {}", err)
        }).or_bail()
    });
    let max_io_threads = args.value_of("max-io-threads").map(|value| {
        value.parse::<usize>().map_err(|_| {
            format!("Invalid max-io-threads argument: '{}' value must be a positive integer", value)
        }).and_then(|parsed| {
            if parsed == 0 {
                Err(format!("max-io-threads cannot be 0"))
            } else {
                Ok(parsed)
            }
        }).or_bail()
    });

    let retention_days = parse_arg_or_exit(&args, "event-retention-days", ::std::i64::MAX);
    let retention_duration = if retention_days == ::std::i64::MAX {
        Duration::max_value()
    } else {
        Duration::days(retention_days)
    };

    let default_eviction_period = ::std::cmp::min(retention_duration.num_hours() / 6, MAX_SEGMENT_PERIOD_HOURS);
    let eviction_period_hours = parse_arg_or_exit(&args, "eviction-period", default_eviction_period);

    let default_election_timeout = ::engine::controller::tick_generator::get_election_timeout_millis();
    let election_timeout = parse_arg_or_exit(&args, "election-timeout", default_election_timeout);
    let heartbeat_interval = parse_arg_or_exit(&args, "heartbeat-interval", election_timeout / 3);

    let server_options = ServerOptions {
        event_retention_duration: retention_duration,
        event_eviction_period: Duration::hours(eviction_period_hours),
        port: port,
        data_dir: data_dir,
        max_cache_memory: max_cache_memory,
        this_instance_address: this_address,
        cluster_addresses: cluster_addresses,
        election_timeout_millis: election_timeout,
        heartbeat_interval_millis: heartbeat_interval,
        max_io_threads: max_io_threads,
    };

    server_options.validate().or_bail();

    let run_finished = server::run(server_options);
    if let Some(err) = run_finished.err() {
        error!("IO Error: {}", err);
        ::std::process::exit(1);
    }
    info!("Shutdown server");
}

fn get_cluster_addresses(args: &ArgMatches) -> Option<Vec<SocketAddr>> {
    args.values_of("join-cluster-address").map(|values| {
        values.flat_map(|address_arg| {
            address_arg.to_socket_addrs()
                    .map_err(|err| {
                        format!("Unable to resolve address: '{}', error: {}", address_arg, err)
                    })
                    .or_bail()
                    .next()
        }).collect()
    })
}

fn get_log_file_option(args: &ArgMatches) -> LogFileOption {
    args.value_of("log-dest").map(|path| {
        LogFileOption::File(Path::new(path).to_path_buf())
    }).unwrap_or(LogFileOption::Stdout)
}

fn get_log_level_options(args: &ArgMatches) -> Vec<LogLevelOption> {
    args.values_of("log-level").map(|level_strs| {
        level_strs.map(|arg_value| {
            LogLevelOption::from_str(arg_value).or_bail()
        }).collect()
    }).unwrap_or(Vec::new())
}

fn get_max_cache_mem_amount(args: &ArgMatches) -> MemoryLimit {
    let mb = parse_arg_or_exit(args, "max-cache-memory", 512usize);
    MemoryLimit::new(mb, MemoryUnit::Megabyte)
}

fn parse_arg_or_exit<T: FromStr + Default>(args: &ArgMatches, arg_name: &str, default: T) -> T {
    args.value_of(arg_name)
        .map(|value| {
            value.parse::<T>().map_err(|_err| {
                format!("argument {} invalid value: {}", arg_name, value)
            }).or_bail()
        })
        .unwrap_or(default)
}

trait ParseArg<T> {
    fn or_bail(self) -> T;
}

impl <T> ParseArg<T> for Result<T, String> {
    fn or_bail(self) -> T {
        match self {
            Ok(value) => value,
            Err(err) => {
                println!("Error: {}", err);
                app_args().print_help().expect("failed to print help message");
                ::std::process::exit(1);
            }
        }
    }
}
