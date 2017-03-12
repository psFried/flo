
extern crate flo_client_lib;

#[macro_use]
extern crate clap;

mod client_cli;


use flo_client_lib::FloEventId;
use clap::{App, Arg, ArgMatches, SubCommand, AppSettings};
use client_cli::{Producer, ProduceOptions, Verbosity, Context, Critical, Consumer, CliConsumerOptions};

use std::net::{SocketAddr, ToSocketAddrs, IpAddr};
use std::str::FromStr;


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

    //consume options
    pub const CONSUME_LIMIT: &'static str = "consume-limit";
    pub const CONSUME_AWAIT: &'static str = "consume-await";
    pub const CONSUME_START_POSITION: &'static str = "consume-start-position";
}

fn create_app_args() -> App<'static, 'static> {
    App::new("flo")
            .version(crate_version!())
            .about("Command line tool for interacting with a flo server")
            .setting(AppSettings::SubcommandRequired)
            .setting(AppSettings::VersionlessSubcommands)
            .arg(Arg::with_name(args::VERBOSE)
                    .short("-v")
                    .long("verbose")
                    .multiple(true)
                    .help("Increase the verbosity of output"))
            .arg(Arg::with_name(args::HOST)
                    .short("H")
                    .long("host")
                    .help("The hostname or ip address of the flo server")
                    .default_value("localhost"))
            .arg(Arg::with_name(args::PORT)
                    .short("p")
                    .long("port")
                    .help("The port number of the flo server")
                    .default_value("3000"))
            .subcommand(SubCommand::with_name(args::PRODUCE)
                    .about("Used to produce events onto the stream")
                    .arg(Arg::with_name(args::NAMESPACE)
                            .short("n")
                            .long("namespace")
                            .help("The namespace to produce the event in. By convention, this is slash separated path (/foo/bar/baz)")
                            .takes_value(true)
                            .required(true))
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
                            .help("The parent id of the event to be produced. Defaults to none") ) )
            .subcommand(SubCommand::with_name(args::CONSUME)
                    .about("Used to read events from the event stream")
                    .arg(Arg::with_name(args::NAMESPACE)
                            .short("n")
                            .long("namespace")
                            .help("The namespace to consume from. Supports glob syntax. Defaults to /**/* which returns all events")
                            .takes_value(true)
                            .default_value("/**/*"))
                    .arg(Arg::with_name(args::CONSUME_START_POSITION)
                            .short("s")
                            .long("start-after")
                            .help("Sets the starting position in the event stream. The first event received will be the on directly AFTER this id")
                            .value_name("EVENT_ID"))
                    .arg(Arg::with_name(args::CONSUME_LIMIT)
                            .short("l")
                            .long("limit")
                            .takes_value(true)
                            .value_name("LIMIT")
                            .help("The maximum number of events to read from the stream. Default behavior is unlimited read") )
                    .arg(Arg::with_name(args::CONSUME_AWAIT)
                            .short("t")
                            .long("tail")
                            .help("Works like tail -f to continuously await new events. Events will be printed as they are received")))
}

fn main() {
    let args = create_app_args().get_matches();

    let context = create_context(&args);
    let host = args.value_of(args::HOST).or_abort_process(&context).to_owned();
    let port = args.value_of(args::PORT).or_abort_process(&context).parse::<u16>().or_abort_with_message("invalid port argument", &context);

    match args.subcommand() {
        (args::PRODUCE, Some(produce_args)) => {
            let parent_id = get_parent_id(&produce_args, &context);
            let event_data = get_event_data(&produce_args);
            let namespace = produce_args.value_of(args::NAMESPACE).or_abort_with_message("Must supply a namespace", &context).to_owned();
            let produce_options = ProduceOptions {
                host: host,
                port: port,
                namespace: namespace,
                event_data: event_data,
                parent_id: parent_id,
            };
            ::client_cli::run::<Producer>(produce_options, context);
        }
        (args::CONSUME, Some(consume_args)) => {
            let start_position = parse_opt_or_exit::<FloEventId>(args::CONSUME_START_POSITION, &consume_args, &context);
            let limit = parse_opt_or_exit::<u64>(args::CONSUME_LIMIT, &consume_args, &context);
            let await = consume_args.is_present(args::CONSUME_AWAIT);
            let namespace = consume_args.value_of(args::NAMESPACE).or_abort_with_message("Must supply a namespace", &context).to_owned();

            let consume_opts = CliConsumerOptions {
                host: host,
                port: port,
                namespace: namespace,
                start_position: start_position,
                limit: limit,
                await: await,
            };

            ::client_cli::run::<Consumer>(consume_opts, context);
        }
        (command, _) => {
            context.abort_process(format!("unknown command: '{}'", command));
        }
    }

}

fn parse_opt_or_exit<T: FromStr>(arg_name: &'static str, args: &ArgMatches, context: &Context) -> Option<T> {
    args.value_of(arg_name).map(|value| {
        value.parse::<T>().map_err(|err| {
            format!("Invalid argument: {}", arg_name)
        }).or_abort_process(&context)
    })
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


