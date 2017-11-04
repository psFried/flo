extern crate url;
extern crate env_logger;
extern crate tempdir;
extern crate flo_client_lib;
extern crate flo_server;
extern crate futures;
extern crate tokio_core;
extern crate chrono;

extern crate log;

use std::fmt::Debug;

use tokio_core::reactor::Core;
use futures::{Stream, Future};

use flo_server::embedded::{EmbeddedFloServer, ControllerOptions, EventStreamOptions, run_embedded_server};

use flo_client_lib::{VersionVector, FloEventId, Event, EventCounter};
use flo_client_lib::codec::{EventCodec, StringCodec};

fn default_test_options() -> EventStreamOptions {
    Default::default()
}

fn codec() -> Box<EventCodec<EventData=String>> {
    Box::new(StringCodec) as Box<EventCodec<EventData=String>>
}


fn integration_test<F>(test_name: &'static str, stream_opts: EventStreamOptions, fun: F) where F: Fn(EmbeddedFloServer, Core) {
    let _ = env_logger::init();
    println!("starting test: {}", test_name);

    let dir_name = test_name.replace("\\w", "-");
    let tmp_dir = tempdir::TempDir::new(&dir_name).expect("failed to create temp dir");

    let controller_options = ControllerOptions {
        storage_dir: tmp_dir.path().to_owned(),
        default_stream_options: stream_opts,
    };
    let embedded_server = run_embedded_server(controller_options).expect("failed to run embedded server");
    let reactor = Core::new().expect("failed to create reactor");

    fun(embedded_server, reactor);
}

fn run_future<T: Debug, E: Debug, F: Future<Item=T, Error=E> + Debug>(reactor: &mut Core, future: F) -> T {
    use tokio_core::reactor::Timeout;
    use futures::future::Either;

    let timeout_millis = 250000;

    let timeout = Timeout::new(::std::time::Duration::from_millis(timeout_millis), &reactor.handle()).unwrap();
    let either_future = timeout.select2(future);
    let either = reactor.run(either_future);
    match either {
        Ok(Either::B((value, _))) => value,
        Ok(Either::A(((), _))) => panic!("future timed out after {} millis", timeout_millis),
        Err(Either::A(_)) => panic!("Error in timeout"),
        Err(Either::B((err, _))) => panic!("Error executing future: {:?}", err),
    }
}


#[test]
fn produce_many_events_then_consume() {
    integration_test("produce many events", default_test_options(), |server, mut reactor| {
        let mut client = server.connect_client::<String>("testy mctesterson".to_owned(), codec(), reactor.handle());
        client = reactor.run(client.connect()).expect("failed to connect client");

        let produce_count = 20usize;
        for i in 0..produce_count {
            let produce = client.produce("/foo", None, format!("event data {}", i));
            let (id, client_again) = reactor.run(produce).expect("failed to produce event");
            client = client_again;
            assert_eq!(i as u64 + 1, id.event_counter);
        }

        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));

        let consume_stream = client.consume("/foo", &vv, Some(produce_count as u64));
        let mut results: Vec<Event<String>> = run_future(&mut reactor, consume_stream.collect());
        assert_eq!(produce_count, results.len());

        let last_event = results.pop().unwrap();
        assert_eq!(FloEventId::new(1, produce_count as EventCounter), last_event.id);
    });
}

#[test]
fn produce_one_event_then_consume_it() {
    integration_test("produce one event", default_test_options(), |server, mut reactor| {
        let client = server.connect_client::<String>("testy mctesterson".to_owned(), codec(), reactor.handle());

        let client = reactor.run(client.connect()).expect("failed to connect client");
        let future = client.produce("/foo/bar", None, "my data".to_owned());

        let (id, client) = reactor.run(future).expect("failed to run produce future");
        println!("produced event: {}", id);

        let mut version_vec = VersionVector::new();
        version_vec.set(FloEventId::new(1, 0));

        let consume_future = client.consume("/foo/*", &version_vec, Some(1));
        let mut event_result = run_future(&mut reactor, consume_future.collect());
        assert_eq!(1, event_result.len());
        assert_eq!(&event_result.pop().unwrap().data, "my data");
    });
}

#[test]
fn startup_embedded_server_and_connect_async_client() {
    integration_test("connect client", default_test_options(), |server, mut reactor| {
        let client = server.connect_client::<String>("testy mctesterson".to_owned(), codec(), reactor.handle());
        assert!(client.current_stream().is_none());
        let future = client.connect();
        let client = reactor.run(future).expect("failed to run connect");
        let stream_name: Option<&str> = client.current_stream().map(|s| s.name.as_ref());
        assert_eq!(Some("default"), stream_name)
    });
}


