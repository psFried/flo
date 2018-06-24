extern crate flo_client_lib;

#[macro_use]
extern crate clap;
extern crate tic;

mod benches;

use std::str::{FromStr};
use std::fmt::{self, Display};
use std::thread::{self, JoinHandle};
use std::time::{Instant, Duration};
use clap::{App, Arg, ArgMatches, AppSettings};
use tic::{Receiver, Interest, Meters, Sender, Clocksource};

use self::benches::ProducerBenchmark;

const HOST: &'static str = "server-host";
const PORT: &'static str = "server-port";


const COMMANDS: &'static str = "commands";
const PRODUCE_COMMAND: &'static str = "produce";
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
        match s.to_ascii_lowercase().as_ref() {
            PRODUCE_COMMAND => Ok(Metric::Produce),
            _ => Err(())
        }
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

    fn run_benchmark(&self, _args: ArgMatches<'static>, address: String, sender: Sender<Metric>, clock: Clocksource, end_time: Instant) {
        match *self {
            Metric::Produce => {
                ProducerBenchmark::new(address.to_owned(), sender, clock, end_time).run().unwrap()
            }
        }
    }
}

fn print_metrics(metric: Metric, recv: &Receiver<Metric>, total_time_secs: u64) {
    use tic::Percentile;
    let meters: Meters<Metric> = recv.clone_meters();
    if let Some(ref count) = meters.count(&metric) {
        println!("{}:", metric);
        let per_sec = **count as f64 / total_time_secs as f64;
        println!("iterations per second: {}", per_sec);
        println!("time per iteration (nanos)");

        for (string, percentile) in vec![("p50", 50.0), ("p90", 90.0), ("p999", 99.9), ("max", 100.0)] {
            let value = meters.percentile(&metric, Percentile(string.to_owned(), percentile)).map(|p| *p).unwrap_or(0);
            println!("{} : {}", string, value);
        }
    }
}

fn create_tic_receiver(args: &ArgMatches) -> (Receiver<Metric>, Duration) {
    let windows = args.value_of(WINDOWS).unwrap().parse::<usize>().or_bail();
    let secs_per_window = args.value_of(SECS_PER_WINDOW).unwrap().parse::<usize>().or_bail();

    let config = Receiver::configure()
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
    let duration = Duration::from_secs(windows as u64 * secs_per_window as u64);
    (recv, duration)
}

fn create_app() -> App<'static, 'static> {
    App::new("flo-bench")
            .version(crate_version!())
            .about("Command line tool for running benchmarks against a flo server")
            .author("Phil Fried")
            .setting(AppSettings::TrailingVarArg)
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
            .arg(Arg::with_name(COMMANDS)
                    .last(true)
                    .value_name("benchmarks")
                    .validator(|input| {
                        Metric::from_str(&input).map(|_| ()).map_err(|_| {
                            format!("Invalid benchmark: '{}'", input)
                        })
                    })
                    .min_values(1))
}

fn main() {
    let app = create_app();
    let args = app.get_matches();

    let (mut receiver, duration) = create_tic_receiver(&args);
    let end_time = Instant::now() + duration;
    let server_address = get_server_address(&args);

    let metrics = args.values_of(COMMANDS).or_bail().map(|value| Metric::from_str(value).or_bail()).collect::<Vec<_>>();

    let threads = metrics.iter().map(|metric| {
        start_bench_thread(*metric, &args, &server_address, &receiver, end_time)
    }).collect::<Vec<_>>();

    receiver.run();
    receiver.save_files();

    println!("Receiver finished");

    for thread in threads {
        thread.join().or_bail();
    }

    println!("All benchmark runners Finished");

    for metric in metrics.iter().collect::<::std::collections::HashSet<_>>() {
        print_metrics(*metric, &receiver, duration.as_secs());
    }
}

fn start_bench_thread(metric: Metric, args: &ArgMatches<'static>, addr: &str, recv: &Receiver<Metric>, end_time: Instant) -> JoinHandle<()> {

    let args = args.clone();
    let address = addr.to_owned();
    let clock = recv.get_clocksource();
    let sender = recv.get_sender();
    thread::Builder::new().name(format!("{}-benchmark", metric)).spawn(move || {
        metric.run_benchmark(args, address, sender, clock, end_time);
    }).or_bail()
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
