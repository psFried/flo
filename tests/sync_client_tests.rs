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
            events: Vec::new()
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


integration_test!{clients_can_connect_and_disconnect_multiple_times_without_making_the_server_barf, port, tcp_stream, {

    fn opts(start: Option<FloEventId>, max_events: u64) -> ConsumerOptions {
        ConsumerOptions {
           namespace: String::new(),
           start_position: start,
           max_events: max_events,
           username: String::new(),
           password: String::new(),
        }
    }

    {
        let mut client = SyncConnection::from_tcp_stream(tcp_stream);
        client.produce("/any/old/name", b"whatever data").expect("failed to produce first event");
    }

    {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create first consumer");
        let mut consumer = TestConsumer::new("consumer one");
        client.run_consumer(opts(None, 1), &mut consumer).expect("failed to run first consumer");
        assert_eq!(1, consumer.events.len());
    }

    let second_event_id = {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create second producer");
        client.produce("/some/other/name", b"whatever data").expect("failed to produce second event")
    };

    {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create second consumer");
        let mut consumer = TestConsumer::new("consumer two");
        client.run_consumer(opts(None, 2), &mut consumer).expect("failed to run second consumer");
        assert_eq!(2, consumer.events.len());
    }

    {
        let mut client = SyncConnection::connect(localhost(port)).expect("failed to create third producer");
        client.produce("/foo/bar/boo/far", b"whatever data").expect("failed to produce third event");
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
