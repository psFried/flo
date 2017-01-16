extern crate flo;
extern crate flo_event;
extern crate url;
extern crate env_logger;
extern crate tempdir;
extern crate byteorder;

#[macro_use]
extern crate nom;

#[macro_use]
mod test_utils;

use test_utils::*;
use flo::client::sync::{SyncConnection, FloConsumer, ConsumerContext, ConsumerAction};
use flo::protocol::{ErrorMessage, ErrorKind};
use flo::client::{ConsumerOptions, ClientError};
use flo_event::{FloEventId, OwnedFloEvent};
use std::thread;
use std::time::Duration;
use std::sync::atomic::{Ordering};
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

    fn on_event(&mut self, event_result: Result<OwnedFloEvent, &ClientError>, _context: &ConsumerContext) -> ConsumerAction {
        match event_result {
            Ok(event) => {
                println!("Consumer {} received event: {:?}", &self.name, event.id);
                self.events.push(event);
                ConsumerAction::Continue
            },
            Err(err) => {
                println!("Consumer: {} - Error reading event #{}: {:?}", &self.name, self.events.len() + 1, err);
                ConsumerAction::Stop
            }
        }
    }
}

integration_test!{consumer_receives_error_after_starting_to_consume_with_invalid_namespace, port, _tcp_stream, {
    let mut client = SyncConnection::connect(localhost(port)).expect("failed to create producer");

    let mut consumer = TestConsumer::new("consumer_receives_error_after_starting_to_consume_with_invalid_namespace");

    let options = ConsumerOptions{
        namespace: "/***".to_owned(),
        start_position: None,
        max_events: 2,
        username: String::new(),
        password: String::new(),
    };
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
    let options = ConsumerOptions {
        namespace: "/animal/mammal/*".to_owned(),
        start_position: None,
        max_events: 2,
        username: String::new(),
        password: String::new(),
    };
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
    let options = ConsumerOptions {
        namespace: namespace.to_owned(),
        start_position: None,
        max_events: 2,
        username: String::new(),
        password: String::new(),
    };
    client.run_consumer(options, &mut consumer).expect("failed to run consumer");

    assert_eq!(2, consumer.events.len());

    for event in consumer.events.iter() {
        assert_eq!(namespace, &event.namespace);
    }

}}

integration_test!{clients_can_connect_and_disconnect_multiple_times_without_making_the_server_barf, port, tcp_stream, {

    let namespace = "/the/test/namespace";
    fn opts(start: Option<FloEventId>, max_events: u64) -> ConsumerOptions {
        ConsumerOptions {
           namespace: "/the/test/namespace".to_owned(),
           start_position: start,
           max_events: max_events,
           username: String::new(),
           password: String::new(),
        }
    }


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

    let num_events = 1000;

    let join_handle = thread::spawn(move || {
        let mut client = SyncConnection::connect(localhost(port)).expect("Failed to create client");

        (0..num_events).map(|i| {
            let event_data = format!("Event: {} data", i);
            client.produce("/the/test/namespace", event_data.as_bytes()).expect("Failed to produce event")
        }).collect::<Vec<FloEventId>>()
    });

    let mut consumer = TestConsumer::new("events are consumed after they are written");
    let mut client = SyncConnection::from_tcp_stream(tcp_stream);

    let options = ConsumerOptions {
        namespace: "/the/test/namespace".to_owned(),
        start_position: None,
        max_events: num_events as u64,
        username: String::new(),
        password: String::new(),
    };

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
