extern crate flo_client_lib;

#[macro_use]
extern crate clap;
extern crate tic;

mod benches;

use std::str::{FromStr};
use std::fmt::{self, Display};
use std::thread::{self, JoinHandle};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Instant, Duration};
use clap::{App, Arg, SubCommand, ArgMatches};
use tic::{Receiver, Config, Interest};

use self::benches::{ProducerBenchmark};

const HOST: &'static str = "server-host";
const PORT: &'static str = "server-port";

const PRODUCE_COMMAND: &'static str = "produce-data-size";
const PRODUCE_DATA_SIZE: &'static str = "produce-data-size";

// generic metrics-related arguments
const WINDOWS: &'static str = "windows";
const SECS_PER_WINDOW: &'static str = "secs-per-window";
const METRICS_OUT_DIR: &'static str = "output-dir";



#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum Metric {
    Produce,
}

impl FromStr for Metric {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        unimplemented!()
    }
}

impl Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let string = match *self {
            Metric::Produce => "Produce",
        };
        write!(f, "{}", string)
    }
}

impl Metric {
    fn add_file_interests(&self, output_dir: &str, receiver: &mut Receiver<Metric>) {
        ::std::fs::create_dir_all(output_dir).or_bail();

        let trace_filename = format!("{}/{}_trace.txt", output_dir, self);
        receiver.add_interest(Interest::Trace(self.clone(), trace_filename));
        let waterfall_file = format!("{}/{}.png", output_dir, self);
        receiver.add_interest(Interest::Waterfall(self.clone(), waterfall_file));
    }
}

fn create_tic_receiver(args: &ArgMatches) -> (Receiver<Metric>, Instant) {
    let windows = args.value_of(WINDOWS).unwrap().parse::<usize>().or_bail();
    let secs_per_window = args.value_of(SECS_PER_WINDOW).unwrap().parse::<usize>().or_bail();

    let mut config = Receiver::configure()
            .windows(windows)
            .duration(secs_per_window)
            .batch_size(128);

    let mut recv = config.build();

    if let Some(file) = args.value_of(METRICS_OUT_DIR) {
        Metric::Produce.add_file_interests(file, &mut recv);
    }

    recv.add_interest(Interest::Count(Metric::Produce));
    recv.add_interest(Interest::AllanDeviation(Metric::Produce));
    recv.add_interest(Interest::Percentile(Metric::Produce));

    println!("running for {} seconds", windows * secs_per_window);
    let end_time = Instant::now() + Duration::from_secs(windows as u64 * secs_per_window as u64);
    (recv, end_time)
}

fn create_app() -> App<'static, 'static> {
    App::new("flo-bench")
            .version(crate_version!())
            .about("Command line tool for running benchmarks against a flo server")
            .author("Phil Fried")
            .arg(Arg::with_name(HOST)
                    .short("H")
                    .long("host")
                    .help("The hostname or ip address of the flo server")
                    .default_value("127.0.0.1"))
            .arg(Arg::with_name(PORT)
                    .short("p")
                    .long("port")
                    .help("The port number of the flo server")
                    .default_value("3000"))
            .arg(Arg::with_name(WINDOWS)
                    .short("w")
                    .long("windows")
                    .help("number of windows to run")
                    .default_value("10"))
            .arg(Arg::with_name(SECS_PER_WINDOW)
                    .short("s")
                    .long("window-seconds")
                    .default_value("10"))
            .arg(Arg::with_name(METRICS_OUT_DIR)
                    .short("o")
                    .long("out")
                    .takes_value(true)
                    .help("output directory for waterfall graph and traces"))
            .arg(Arg::with_name(PRODUCE_DATA_SIZE)
                    .long("size")
                    .default_value("1024")
                    .help("size of each event body in bytes"))
}

fn main() {
    let mut app = create_app();
    let args = app.get_matches();

    let (mut receiver, end_time) = create_tic_receiver(&args);
    let server_address = get_server_address(&args);

    let data_size = args.value_of(PRODUCE_DATA_SIZE).or_bail().parse::<usize>().or_bail();

    let produce_bench = ProducerBenchmark::new(server_address, receiver.get_sender(), receiver.get_clocksource(), end_time);


    let bench_thread = thread::spawn(move || {
        println!("Starting Producer");
        let result = produce_bench.run();
        println!("Finished producer with result: {:?}", result);
        result
    });

    receiver.run();
    receiver.save_files();

    println!("Receiver finished");
    bench_thread.join().or_bail();

    println!("All benchmark runners Finished");
}

fn get_server_address(args: &ArgMatches) -> String {
    let port = args.value_of(PORT).unwrap().parse::<u16>().or_bail();
    let address = args.value_of(HOST).unwrap();
    format!("{}:{}", address, port)
}



trait ParseArg<T> {
    fn or_bail(self) -> T;
}

impl <T> ParseArg<T> for Option<T> {
    fn or_bail(self) -> T {
        match self {
            Some(t) => t,
            None => {
                create_app().print_help().unwrap();
                println!("Error: missing required value");
                ::std::process::exit(1);
            }
        }
    }
}

impl <T, E> ParseArg<T> for Result<T, E> where E: ::std::fmt::Debug {
    fn or_bail(self) -> T {
        match self {
            Ok(value) => value,
            Err(err) => {
                create_app().print_help().unwrap();
                println!("Error: {:?}", err);
                ::std::process::exit(1);
            }
        }
    }
}
