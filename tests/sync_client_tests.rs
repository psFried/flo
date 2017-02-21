extern crate flo_sync_client;
extern crate url;
extern crate env_logger;
extern crate tempdir;
extern crate byteorder;

#[macro_use]
extern crate nom;

#[macro_use]
mod test_utils;

use test_utils::*;
use flo_sync_client::client::sync::{SyncConnection, FloConsumer, ConsumerContext, ConsumerAction};
use flo_sync_client::protocol::ErrorKind;
use flo_sync_client::client::{ConsumerOptions, ClientError};
use flo_sync_client::{FloEventId, OwnedFloEvent};
use std::thread;
use std::time::Duration;
use std::net::{TcpStream, SocketAddr, SocketAddrV4, Ipv4Addr};



fn localhost(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port)
}

struct TestConsumer{
    name: String,
    events: Vec<OwnedFloEvent>,
}

impl TestConsumer {
    pub fn new(name: &str) -> TestConsumer  {
        TestConsumer{
            name: name.to_owned(),
            events: Vec::new(),
        }
    }
}

impl FloConsumer for TestConsumer {
    fn name(&self) -> &str {
        &self.name
    }

    fn on_event<C: ConsumerContext>(&mut self, event_result: Result<OwnedFloEvent, &ClientError>, _context: &mut C) -> ConsumerAction {
        match event_result {
            Ok(event) => {
                println!("Consumer {} received event: {:?}", &self.name, event);
                self.events.push(event);
                ConsumerAction::Continue
            },
            Err(ref err) if err.is_end_of_stream() => {
                println!("reached end of stream for: '{}'", &self.name);
                ConsumerAction::Continue
            }
            Err(err) => {
                println!("Consumer: {} - Error reading event #{}: {:?}", &self.name, self.events.len() + 1, err);
                ConsumerAction::Stop
            }
        }
    }
}

#[test]
fn consumer_transitions_from_reading_events_from_disk_to_reading_from_memory() {
    use test_utils::{FloServerProcess, init_logger, get_server_port, ServerProcessType};
    use tempdir::TempDir;

    init_logger();
    let mut server_process = None;
    let (proc_type, port) = get_server_port();
    if proc_type == ServerProcessType::Child {
        let tempdir = TempDir::new("flo-integraion-test-transitions").unwrap();
        let args = vec!["--max-cached-events".to_owned(), "5".to_owned()];
        server_process = Some(FloServerProcess::with_args(port, tempdir, args));
    }

    let mut client = SyncConnection::connect(localhost(port)).expect("failed to connect");
    for i in 0..10 {
        client.produce("/the/namespace", format!("event data: {}", i)).expect("Failed to produce event");
    }

    let mut consumer = TestConsumer::new("consumer_transitions_from_reading_events_from_disk_to_reading_from_memory");
    let options = ConsumerOptions::simple("/the/namespace", None, 10);
    client.run_consumer(options, &mut consumer).expect("running consumer failed");

    assert_eq!(10, consumer.events.len());

    server_process.map(|mut process| {
        // explicitly kill the server here to ensure that it stays in scope since it's automatically killed on Drop
        process.kill();
    });
}

integration_test!{consumer_responds_to_event, port, _tcp_stream, {
    let mut client = SyncConnection::connect(localhost(port)).expect("failed to create producer");

    struct RespondingConsumer;
    impl FloConsumer for RespondingConsumer {
        fn name(&self) -> &str {
            "consumer responds to event"
        }
        fn on_event<C: ConsumerContext>(&mut self, event_result: Result<OwnedFloEvent, &ClientError>, context: &mut C) -> ConsumerAction {
            println!("respondingConsumer got event: {:?}", event_result);
            if let Ok(event) = event_result {
                context.respond("/responses", event.data).into()
            } else {
                ConsumerAction::Stop
            }
        }
    }

    let event_1_id = client.produce("/events", b"data").unwrap();
    let event_2_id = client.produce("/events", b"data 2").unwrap();
    let mut consumer = RespondingConsumer;
    let options = ConsumerOptions::simple("/events", None, 2);
    client.run_consumer(options, &mut consumer).expect("failed to run consumer");

    let mut consumer = TestConsumer::new("verify that response events exist");
    let options = ConsumerOptions::simple("/responses", None, 2);
    client.run_consumer(options, &mut consumer).expect("failed to run second consumer");

    assert_eq!(2, consumer.events.len());
    let response_1 = consumer.events.remove(0);
    assert_eq!("/responses", &response_1.namespace);
    assert_eq!(Some(event_1_id), response_1.parent_id);

    let response_2 = consumer.events.remove(0);
    assert_eq!("/responses", &response_2.namespace);
    assert_eq!(Some(event_2_id), response_2.parent_id);
}}

integration_test!{consumer_receives_error_after_starting_to_consume_with_invalid_namespace, port, _tcp_stream, {
    let mut client = SyncConnection::connect(localhost(port)).expect("failed to create producer");

    let mut consumer = TestConsumer::new("consumer_receives_error_after_starting_to_consume_with_invalid_namespace");

    let options = ConsumerOptions::simple("/***", None, 2);
    let result = client.run_consumer(options, &mut consumer);

    assert!(result.is_err());

    match result.unwrap_err() {
        ClientError::FloError(err) => {
            assert_eq!(ErrorKind::InvalidNamespaceGlob, err.kind);
        }
        other @ _ => {
            panic!("Expected FloError, got: {:?}", other);
        }
    }
}}

integration_test!{consumer_reads_events_matching_glob_pattern, port, tcp_stream, {
    let mut client = SyncConnection::connect(localhost(port)).expect("failed to create producer");

    client.produce("/animal/mammal/koala", b"data").expect("failed to produce event");
    client.produce("/animal/mammal/sorta/platypus", b"data").expect("failed to produce event");
    client.produce("/animal/reptile/snake", b"data").expect("failed to produce event");
    client.produce("/animal/mammal/mouse", b"data").expect("failed to produce event");
    client.produce("/animal/bird/magpie", b"data").expect("failed to produce event");

    let mut consumer = TestConsumer::new("consumer_reads_events_matching_glob_pattern");
    let options = ConsumerOptions::simple("/animal/mammal/*", None, 2);
    client.run_consumer(options, &mut consumer).expect("failed to run consumer");

    assert_eq!(2, consumer.events.len());

    assert_eq!("/animal/mammal/koala", consumer.events[0].namespace);
    assert_eq!("/animal/mammal/mouse", consumer.events[1].namespace);
}}

integration_test!{consumer_only_receives_events_with_exactly_matching_namespace, port, tcp_stream, {

    let mut client = SyncConnection::connect(localhost(port)).expect("failed to create producer");

    let namespace = "/test/namespace";

    client.produce("/wrong/namespace", b"wrong data").expect("failed to produce event");
    client.produce("/test/namespace/extra", b"wrong data").expect("failed to produce event");
    client.produce(namespace, b"right data").expect("failed to produce event");
    client.produce("/wrong/namespace", b"wrong data").expect("failed to produce event");
    client.produce(namespace, b"right data").expect("failed to produce event");

    let mut consumer = TestConsumer::new("consumer_only_receives_events_with_exactly_matching_namespace");
    let options = ConsumerOptions::simple(namespace, None, 2);
    client.run_consumer(options, &mut consumer).expect("failed to run consumer");

    assert_eq!(2, consumer.events.len());

    for event in consumer.events.iter() {
        assert_eq!(namespace, &event.namespace);
    }

}}

integration_test!{clients_can_connect_and_disconnect_multiple_times_without_making_the_server_barf, port, tcp_stream, {

    let namespace = "/the/test/namespace";
    let opts = |start: Option<FloEventId>, max_events: u64| {
        ConsumerOptions::simple(namespace, start, max_events)
    };


    {
        let mut client = SyncConnection::from_tcp_stream(tcp_stream);
        client.produce(namespace, b"whatever data").expect("failed to produce first event");
    }

    {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create first consumer");
        let mut consumer = TestConsumer::new("consumer one");
        client.run_consumer(opts(None, 1), &mut consumer).expect("failed to run first consumer");
        assert_eq!(1, consumer.events.len());
    }

    let second_event_id = {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create second producer");
        client.produce(namespace, b"whatever data").expect("failed to produce second event")
    };

    {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create second consumer");
        let mut consumer = TestConsumer::new("consumer two");
        client.run_consumer(opts(None, 2), &mut consumer).expect("failed to run second consumer");
        assert_eq!(2, consumer.events.len());
    }

    {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create third producer");
        client.produce(namespace, b"whatever data").expect("failed to produce third event");
    }

    {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create third consumer");
        let mut consumer = TestConsumer::new("consumer three");
        client.run_consumer(opts(Some(second_event_id), 1), &mut consumer).expect("failed to run third consumer");
        assert_eq!(1, consumer.events.len());
    }
}}

integration_test!{many_events_are_produced_using_sync_client, port, tcp_stream, {

    let mut client = SyncConnection::connect(localhost(port)).expect("failed to create client");

    for i in 0..1000 {
        match client.produce("/any/ns", b"some bytes") {
            Ok(_) => {}
            Err(err) => {
                panic!("Failed to produce event: {}, err: {:?}", i, err);
            }
        }
    }
}}


integration_test!{events_are_consumed_as_they_are_written, port, tcp_stream, {

    let num_events: usize = 3;

    let join_handle = thread::spawn(move || {
        let mut client = SyncConnection::connect(localhost(port)).expect("Failed to create client");

        (0..num_events).map(|i| {
            let event_data = format!("Event: {} data", i);
            client.produce("/the/test/namespace", event_data.as_bytes()).expect("Failed to produce event")
        }).collect::<Vec<FloEventId>>()
    });

    let mut consumer = TestConsumer::new("events are consumed as they are written");
    let mut client = SyncConnection::from_tcp_stream(tcp_stream);

    let options = ConsumerOptions::simple("/the/test/namespace", None, num_events as u64);
    let result = client.run_consumer(options, &mut consumer);

    let produced_ids = join_handle.join().expect("Producer thread panicked!");
    assert_eq!(num_events, produced_ids.len(), "didn't produce correct number of events");

    assert_eq!(num_events, consumer.events.len(), "didn't consume enough events");

    result.expect("Error running consumer");
}}

integration_test!{event_is_written_using_sync_connection, port, tcp_stream, {
    let mut client = SyncConnection::connect(localhost(port)).expect("failed to connect");
    client.produce("/this/namespace/is/boss", b"this is the event data").unwrap();
}}
