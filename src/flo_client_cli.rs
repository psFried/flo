
extern crate flo_sync_client;

#[macro_use]
extern crate clap;

mod client_cli;


use clap::{App, Arg, ArgMatches, SubCommand};
use client_cli::error::Critical;

use std::net::{SocketAddr, ToSocketAddrs, IpAddr};


mod args {
    //sub-commands
    pub const PRODUCE: &'static str = "produce";
    pub const CONSUME: &'static str = "consume";

    //global options
    pub const VERBOSE: &'static str = "verbose";
    pub const HOST: &'static str = "server-host";
    pub const PORT: &'static str = "server-port";
    pub const NAMESPACE: &'static str = "namespace";

    //produce options
    pub const EVENT_DATA: &'static str = "event-data";
    pub const PARENT_ID: &'static str = "parent-id";
}

fn create_app_args() -> App<'static, 'static> {
    App::new("flo")
            .version(crate_version!())
            .about("Command line tool for interacting with a flo server")
            .arg(Arg::with_name(args::VERBOSE)
                    .short("-v")
                    .multiple(true)
                    .help("Increase the verbosity of output"))
            .arg(Arg::with_name(args::HOST)
                    .short("h")
                    .long("host")
                    .help("The hostname or ip address of the flo server")
                    .default_value("localhost"))
            .arg(Arg::with_name(args::PORT)
                    .short("p")
                    .long("port")
                    .help("The port number of the flo server")
                    .default_value("3000"))
            .arg(Arg::with_name("namespace")
                    .short("n")
                    .long("namespace")
                    .help("The namespace to consume from or produce to")
                    .required(true))
            .subcommand(SubCommand::with_name(args::PRODUCE)
                    .arg(Arg::with_name(args::EVENT_DATA)
                            .short("d")
                            .long("data")
                            .help("The event data to produce. If supplied multiple times, multiple events will be produced in order")
                            .default_value(""))
                    .arg(Arg::with_name(args::PARENT_ID)
                            .short("P")
                            .long("parent")
                            .help("The parent id of the event to be produced. Defaults to none")
                    )
            )
}

fn main() {
    let args = create_app_args().get_matches();

    let address = resolve_address(&args);


    match args.subcommand() {
        (args::PRODUCE, Some(produce_args)) => {

        }
    }

}

fn resolve_address(args: &ArgMatches) -> SocketAddr {
    let port = args.values_of(args::PORT)
                   .or_abort_process()
                   .parse::<u16>()
                   .or_abort_with_message("invalid port number");

    let host = args.value_of(args::HOST).or_abort_process();

    let address_iter = (host, port).to_socket_addrs().map_err(|io_err| {
        format!("Unable to resolve address: {}", io_err)
    }).or_abort_process();
    address_iter.next().or_abort_with_message("Invalid address")
}


