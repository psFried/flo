extern crate flo_client_lib;
extern crate url;
extern crate flo_server;
extern crate tempdir;

use std::fmt::Debug;
use std::time::Duration;
use std::net::{SocketAddrV4, Ipv4Addr};
use std::thread;

mod test_utils;

use test_utils::*;
use flo_client_lib::sync::{SyncConnection, EventToProduce};
use flo_client_lib::{FloEventId, VersionVector};
use flo_client_lib::codec::StringCodec;

fn localhost(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port)
}

fn simple_event<N: Into<String>, D: Debug, E: Into<D>>(namespace: N, data: E) -> EventToProduce<D> {
    EventToProduce {
        partition: 1,
        namespace: namespace.into(),
        parent_id: None,
        data: data.into()
    }
}

fn test_with_server<F: FnOnce(u16)>(test_name: &'static str, flo_server_args: Vec<&str>, test_fun: F) {
    use tempdir::TempDir;

    init_logger();

    let (proc_type, port) = get_server_port();
    let temp_dir = TempDir::new(test_name).unwrap();
    let mut server_process = None;

    if proc_type == ServerProcessType::Child {
        let args = flo_server_args.iter().map(|a| a.to_string()).collect::<Vec<_>>();
        server_process = Some(FloServerProcess::with_args(port, temp_dir, args));
    }

    test_fun(port);

    if let Some(mut process) = server_process {
        process.kill();
    }
}

fn vv_from_start() -> VersionVector {
    let mut vv = VersionVector::new();
    vv.set(FloEventId::new(1, 0));
    vv
}

#[test]
fn consumer_stops_early_and_connection_is_returned() {
    test_with_server("consumer_stops_early", Vec::new(), |port| {
        let mut connection = SyncConnection::connect(localhost(port), "large event", StringCodec, None).expect("failed to connect");

        for _ in 0..5 {
            connection.produce_to(1, "/test", None, String::new())
                    .expect("failed to produce event");
        }

        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));
        // start consuming without any limit
        let mut first_consume = connection.into_consumer("/test", &vv, None, true);

        let event = first_consume.next().expect("next returned None").expect("first consume failed");
        assert_eq!(FloEventId::new(1, 1), event.id);
        connection = first_consume.stop_consuming().expect("failed to stop consuming");

        let mut second_consume = connection.into_consumer("/test", &vv, None, true);
        let event = second_consume.next().expect("next returned None").expect("second consume failed");
        assert_eq!(FloEventId::new(1, 1), event.id);
    });
}

#[test]
fn produce_then_consume_one_large_event() {
    test_with_server("consumer_produces_and_consumes_a_large_event", Vec::new(), |port| {
        let mut connection = SyncConnection::connect(localhost(port), "large event", ::flo_client_lib::codec::RawCodec, None).expect("failed to connect");

        let event_size = 1024 * 1024;
        let event_data = ::std::iter::repeat(88u8).take(event_size).collect::<Vec<_>>();
        connection.produce(simple_event("/foo", event_data)).expect("failed to produce event");

        let vv = vv_from_start();

        let result = connection.into_consumer("/*", &vv, Some(1), false).next()
                .expect("Event was None!")
                .expect("Error reading event");

        assert_eq!(event_size, result.data.len());
    })
}

#[test]
fn produce_many_events_then_consume_them_in_batches() {
    test_with_server("produce many events then consume them", Vec::new(), |port| {
        let mut connection = SyncConnection::connect(localhost(port), "produceMany", StringCodec, Some(10)).expect("failed to connect");

        let event_count = 200;

        for i in 0..event_count {
            let event = EventToProduce::new(1, "/foo", None, format!("event {} data", i));
            let _ = connection.produce(event).expect("failed to produce event");
        }
        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));
        let iter = connection.into_consumer("/*", &vv, None, false);
        let received_count = iter.map(|result| result.unwrap()).count();
        assert_eq!(event_count, received_count);
    });
}

#[test]
fn consumer_receives_events_as_they_are_produced() {
    test_with_server("receive events as they are produced", Vec::new(), |port| {
        let mut producer_conn = SyncConnection::connect(localhost(port), "producer", StringCodec, None).expect("failed to connect");

        let id_1 = producer_conn.produce(simple_event("/foo", "foo".to_owned()))
                .expect("failed to produce first event");


        let join_handle = thread::spawn(move || {
            let consumer_conn = SyncConnection::connect(localhost(port), "consumer", StringCodec, None).expect("failed to connect");
            let iter = consumer_conn.into_consumer("/*", &vv_from_start(), Some(3), true);

            iter.map(|result| {
                result.expect("failed to receive event").id
            }).collect::<Vec<FloEventId>>()
        });

        thread::sleep(Duration::from_millis(500));

        let id_2 = producer_conn.produce(simple_event("/bar", "bar".to_owned())).expect("failed to produce second event");
        let id_3 = producer_conn.produce(simple_event("/baz", "baz".to_owned())).expect("failed to produce third event");

        let received_ids = join_handle.join().expect("failed to run consumer");
        assert_eq!(vec![id_1, id_2, id_3], received_ids);
    });
}

#[test]
fn consumer_completes_successfully_when_it_reaches_end_of_stream_even_when_event_limit_is_high() {
    test_with_server("stop at end of stream, even when event limit has not been reached", Vec::new(), |port| {
        let mut connection = SyncConnection::connect(localhost(port), "stopAtEnd", StringCodec, None).expect("failed to connect");

        let event_count = 20;

        for i in 0..event_count {
            let event = EventToProduce::new(1, "/foo", None, format!("event {} data", i));
            let _ = connection.produce(event).expect("failed to produce event");
        }
        let vv = vv_from_start();
        let iter = connection.into_consumer("/*", &vv, None, false);

        let count = iter.map(|result| result.unwrap()).count();
        assert_eq!(event_count, count);
    });
}

fn connect_and_consume(port: u16, namespace: &str) -> Vec<FloEventId> {
    let connection = SyncConnection::connect(localhost(port), "connect_and_consume", StringCodec, None).expect("failed to connect");

    let vv = vv_from_start();

    connection.into_consumer(namespace, &vv, None, false)
        .map(|result| result.expect("failed to receive event").id)
        .collect()
}

#[test]
fn consumer_receives_only_events_matching_namespace_glob() {
    test_with_server("consumer namespace globs", Vec::new(), |port| {
        let mut connection = SyncConnection::connect(localhost(port), "stopAtEnd", StringCodec, None).expect("failed to connect");

        let mut produce = |namespace: &str| {
            connection.produce_to(1, namespace, None, String::new()).expect("failed to produce event")
        };

        let _meals = produce("/meals");
        let breakfast = produce("/meals/breakfast");
        let bacon = produce("/meals/breakfast/foods/bacon");
        let eggs = produce("/meals/breakfast/foods/eggs");
        let coffee = produce("/meals/breakfast/drinks/coffee");

        let _lunch = produce("/meals/lunch");
        let _hamburgers = produce("/meals/lunch/foods/hamburgers");
        let soda = produce("/meals/lunch/drinks/soda");

        let results = connect_and_consume(port, "/meals/breakfast/foods/*");
        assert_eq!(vec![bacon, eggs], results);

        let results = connect_and_consume(port, "/**/drinks/*");
        assert_eq!(vec![coffee, soda], results);

        let results = connect_and_consume(port, "/meals/breakfast");
        assert_eq!(vec![breakfast], results);
    });
}

