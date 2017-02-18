
extern crate flo_sync_client;

#[macro_use]
extern crate clap;

mod client_cli;


use flo_sync_client::FloEventId;
use clap::{App, Arg, ArgMatches, SubCommand};
use client_cli::{Producer, ProduceOptions, Verbosity, Context, Critical};

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
                    .takes_value(true)
                    .required(true))
            .subcommand(SubCommand::with_name(args::PRODUCE)
                    .arg(Arg::with_name(args::EVENT_DATA)
                            .short("d")
                            .long("data")
                            .help("The event data to produce. If supplied multiple times, multiple events will be produced in order")
                            .multiple(true)
                            .default_value(""))
                    .arg(Arg::with_name(args::PARENT_ID)
                            .short("P")
                            .long("parent")
                            .takes_value(true)
                            .value_name("PARENT-EVENT-ID")
                            .help("The parent id of the event to be produced. Defaults to none")
                    )
            )
}

fn main() {
    let args = create_app_args().get_matches();

    let context = create_context(&args);
    let host = args.value_of(args::HOST).or_abort_process(&context);
    let port = args.value_of(args::PORT).or_abort_process(&context).parse::<u16>().or_abort_with_message("invalid port argument", &context);
    let namespace = args.value_of(args::NAMESPACE).or_abort_with_message("Must supply a namespace", &context);

    match args.subcommand() {
        (args::PRODUCE, Some(produce_args)) => {
            let parent_id = get_parent_id(&produce_args, &context);
            let event_data = get_event_data(&produce_args);
            let produce_options = ProduceOptions {
                host: host.to_owned(),
                port: port,
                namespace: namespace.to_owned(),
                event_data: event_data,
                parent_id: parent_id,
            };
            ::client_cli::run::<Producer>(produce_options, context);
        }
        (command, _) => {
            context.abort_process(format!("unknown command: '{}'", command));
        }
    }

}

fn create_context(args: &ArgMatches) -> Context {
    let verbosity = match args.occurrences_of(args::VERBOSE) {
        0 => Verbosity::Normal,
        1 => Verbosity::Verbose,
        _ => Verbosity::Debug
    };
    Context::new(verbosity)
}

fn get_parent_id(args: &ArgMatches, context: &Context) -> Option<FloEventId> {
    args.value_of(args::PARENT_ID).map(|parent_id_string| {
        parent_id_string.parse::<FloEventId>().or_abort_process(context)
    })
}

fn get_event_data(args: &ArgMatches) -> Vec<Vec<u8>> {
    args.values_of(args::EVENT_DATA).map(|values| {
        values.map(|str_val| str_val.as_bytes().to_owned()).collect()
    }).unwrap_or(Vec::new())
}


