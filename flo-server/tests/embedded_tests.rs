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
use std::thread;
use std::time::Duration;

use tokio_core::reactor::Core;
use futures::{Stream, Future};

use flo_server::embedded::{EmbeddedFloServer, ControllerOptions, EventStreamOptions, run_embedded_server};

use flo_client_lib::{VersionVector, FloEventId, Event, EventCounter, ActorId};
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
    let reactor = Core::new().expect("failed to create reactor");
    let embedded_server = run_embedded_server(controller_options, reactor.remote()).expect("failed to run embedded server");

    fun(embedded_server, reactor);
}

fn run_future<T: Debug, E: Debug, F: Future<Item=T, Error=E> + Debug>(reactor: &mut Core, future: F) -> T {
    use tokio_core::reactor::Timeout;
    use futures::future::Either;

    let timeout_millis = 1000;

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
fn consumer_stops_and_restarts_consuming() {
    integration_test("stop_and_restart_consuming", default_test_options(), |server, mut reactor| {
        let mut connection = server.connect_client::<String>("restart_consumer".to_owned(), codec(), reactor.handle());
        connection = reactor.run(connection.connect()).expect("failed to connect consumer");

        for _ in 0..5 {
            let prod = connection.produce_to(1, "/test", None, "some data".to_owned());
            let (_, c) = run_future(&mut reactor, prod);
            connection = c;
        }

        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));

        // start consuming without a limit and without stopping at the end of the stream
        let consumer = connection.consume("/test", &vv, None, true);

        // read one event from the stream and then have the consumer stop, returning the connection for re-use
        let future = consumer.into_future()
                .map_err(|err| err.0.error)
                .and_then(|(_, consumer)| {
                    consumer.stop()
                });
        let connection = reactor.run(future).expect("failed to run first consume");

        // now expect that the connection can be reused
        let consumer = connection.consume("/test", &vv, None, true);
        let next_consume = consumer.into_future();
        let (maybe_event, _) = reactor.run(next_consume).expect("failed to run second consume");
        let event = maybe_event.expect("next event was None");

        // Assert that we get the first event again
        assert_eq!(FloEventId::new(1, 1), event.id);
    });
}

#[test]
fn oldest_events_are_dropped_from_beginning_of_stream_after_time_based_expiration() {
    let retention_duration = chrono::Duration::milliseconds(300);
    let segment_duration = chrono::Duration::milliseconds(100);
    let options = EventStreamOptions {
        event_retention: retention_duration,
        max_segment_duration: segment_duration,
        segment_max_size_bytes: 999999,
        ..Default::default()
    };
    integration_test("drop oldest events by time", options, |server, mut reactor| {
        let mut connection = server.connect_client::<String>("producer".to_owned(), codec(), reactor.handle());
        connection = reactor.run(connection.connect()).expect("failed to connect producer");

        let start_time = ::std::time::Instant::now();
        while start_time.elapsed() < retention_duration.to_std().unwrap() {
            let future = connection.produce_to(1, "/foo", None, String::new());
            let (_, conn) = run_future(&mut reactor, future);
            connection = conn;
        }

        let start_time = ::std::time::Instant::now();
        let until_all_expired = (retention_duration + segment_duration + chrono::Duration::milliseconds(250)).to_std().unwrap();
        let mut first_event_id = FloEventId::new(1, 0);
        let mut vv = VersionVector::new();
        vv.set(first_event_id);

        while start_time.elapsed() < until_all_expired {
            let future = connection.consume("/*", &vv, Some(1), false).into_future();
            let (maybe_event, stream) = run_future(&mut reactor, future);
            connection = stream.into();
            if let Some(event) = maybe_event {
                assert!(event.id >= first_event_id);
                first_event_id = event.id;
            } else {
                break;
            }
        }
        let future = connection.consume("/*", &vv, Some(1), false).into_future();
        let (maybe_event, _) = run_future(&mut reactor, future);
        assert!(maybe_event.is_none(), "Expected events to all be gone, first event id was: {}", first_event_id);
    });
}

#[test]
fn consumer_stops_at_end_of_stream_when_await_new_events_is_false() {
    integration_test("consumer stops at end of stream when await new events is false", default_test_options(), |server, mut reactor| {

        let mut connection = server.connect_client::<String>("producer".to_owned(), codec(), reactor.handle());
        connection = reactor.run(connection.connect()).expect("failed to connect producer");

        let produce = connection.produce_to(1, "/foo", None, "some data".to_owned());
        let (_, connection) = run_future(&mut reactor, produce);

        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));
        let stream = connection.consume("/foo", &vv, None, false);

        // should complete more or less immediately and not time out waiting for new events
        let results = run_future(&mut reactor, stream.collect());
        assert_eq!(1, results.len());
    });
}

#[test]
fn consumer_can_read_events_from_multiple_partitions() {
    let partition_count = 3;
    let options = EventStreamOptions {
        num_partitions: partition_count,
        ..Default::default()
    };
    integration_test("multiple partitions", options, |server, mut reactor| {
        let mut client = server.connect_client::<String>("batch client".to_owned(), codec(), reactor.handle());
        client = reactor.run(client.connect()).expect("failed to connect batch client");

        let mut expected_ids = Vec::with_capacity(50);

        for i in 0..50 {
            let partition = (i % partition_count) as ActorId + 1;
            let prod = client.produce_to(partition, "/test", None, "some data".to_owned());
            let (id, c) = run_future(&mut reactor, prod);
            assert_eq!(partition, id.actor);
            expected_ids.push(id);
            client = c;
        }

        let mut vv = VersionVector::new();
        for i in 0..partition_count {
            vv.set(FloEventId::new(i + 1, 0));
        }

        let consume = client.consume("/test", &vv, Some(50), false);
        let events = run_future(&mut reactor, consume.collect());
        let actual_ids = events.iter().map(|e| e.id).collect::<Vec<FloEventId>>();
        assert_eq!(expected_ids, actual_ids);
    });
}

#[test]
fn consumer_reads_events_in_batches() {
    integration_test("consumer reads events in batches", default_test_options(), |server, mut reactor| {
        let mut client = server.connect_client::<String>("batch client".to_owned(), codec(), reactor.handle());
        client = reactor.run(client.connect_with(Some(10))).expect("failed to connect batch client");

        let event_count = 100usize;
        for _ in 0..event_count {
            let produce = client.produce_to(1, "/test", None, "event data".to_owned());
            let (_, c) = run_future(&mut reactor, produce);
            client = c;
        }
        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));
        let consume = client.consume("/test", &vv, Some(event_count as u64), false);
        let events = run_future(&mut reactor, consume.collect());
        assert_eq!(event_count, events.len());
    });
}

#[test]
fn consumer_receives_only_events_matching_glob() {
    integration_test("consumer receives events matching glob", default_test_options(), |server, mut reactor| {
        let mut client = server.connect_client::<String>("globProducer".to_owned(), codec(), reactor.handle());
        client = reactor.run(client.connect()).expect("failed to connect producer");

        let event_namespaces = vec![
            "/foo",
            "/bar",
            "/foo/bar/baz",
            "/foo/bar",
            "/who/bar/qux"
        ];

        for ns in event_namespaces {
            let produce = client.produce_to(1, ns, None, "event data".to_owned());
            let (_, client_to_reuse) = run_future(&mut reactor, produce);
            client = client_to_reuse;
        }

        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));
        let consumer = client.consume("/**/bar/*", &vv, Some(2), false);
        let events = run_future(&mut reactor, consumer.collect());

        let actual_namespaces = events.iter().map(|event| event.namespace.to_string()).collect::<Vec<String>>();
        let expected_namespaces = vec!["/foo/bar/baz", "/who/bar/qux"];
        assert_eq!(expected_namespaces, actual_namespaces);
    });
}

#[test]
fn consumer_receives_event_as_it_is_produced() {
    integration_test("consumer receives event as it is produced", default_test_options(), |server, mut reactor| {

        let mut producer_client = server.connect_client::<String>("producer".to_owned(), codec(), reactor.handle());
        producer_client = reactor.run(producer_client.connect()).expect("failed to connect producer");

        let join_handle = thread::spawn(move || {
            let mut consumer_core = Core::new().unwrap();

            let mut consumer_client = server.connect_client::<String>("consumer".to_owned(),
                                                                      codec(),
                                                                      consumer_core.handle());
            consumer_client = consumer_core.run(consumer_client.connect()).expect("failed to connect consumer");

            let mut vv = VersionVector::new();
            vv.set(FloEventId::new(1, 0));
            let stream = consumer_client.consume("/foo", &vv, Some(1), true);
            println!("About to run consumer");
            let result = run_future(&mut consumer_core, stream.collect());
            println!("Finished consumer thread");
            result
        });

        println!("started consumer thread");
        thread::sleep(Duration::from_millis(50));

        let produce = producer_client.produce_to(1, "/foo", None, "some data".to_owned());
        println!("about to run produce future");
        let (id, _) = reactor.run(produce).expect("failed to produce event");
        println!("finished produce future");

        let mut consumed_events = join_handle.join().expect("failed to run consumer");
        assert_eq!(1, consumed_events.len());

        let result_event = consumed_events.pop().unwrap();
        assert_eq!(id, result_event.id);
    });
}

#[test]
fn produce_many_events_then_consume() {
    integration_test("produce many events", default_test_options(), |server, mut reactor| {
        let mut client = server.connect_client::<String>("testy mctesterson".to_owned(), codec(), reactor.handle());
        client = reactor.run(client.connect()).expect("failed to connect client");

        let produce_count = 20usize;
        for i in 0..produce_count {
            let produce = client.produce_to(1, "/foo", None, format!("event data {}", i));
            let (id, client_again) = reactor.run(produce).expect("failed to produce event");
            client = client_again;
            assert_eq!(i as u64 + 1, id.event_counter);
        }

        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));

        let consume_stream = client.consume("/foo", &vv, Some(produce_count as u64), false);
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
        let future = client.produce_to(1, "/foo/bar", None, "my data".to_owned());

        let (id, client) = reactor.run(future).expect("failed to run produce future");
        println!("produced event: {}", id);

        let mut version_vec = VersionVector::new();
        version_vec.set(FloEventId::new(1, 0));

        let consume_future = client.consume("/foo/*", &version_vec, Some(1), false);
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


