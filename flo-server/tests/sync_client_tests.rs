extern crate flo_client_lib;
extern crate url;
extern crate env_logger;
extern crate tempdir;

#[macro_use]
mod test_utils;

use test_utils::*;
use flo_client_lib::sync::connection::{SyncConnection, ConsumerOptions};
use flo_client_lib::sync::{Consumer, Context, ConsumerAction, ClientError};
use flo_client_lib::{FloEventId, Event, ErrorKind, VersionVector};
use flo_client_lib::codec::StringCodec;
use std::thread;
use std::time::Duration;
use std::net::{TcpStream, SocketAddr, SocketAddrV4, Ipv4Addr};

fn localhost(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port)
}

struct TestConsumer{
    name: String,
    events: Vec<Event<String>>,
}

impl TestConsumer {
    pub fn new(name: &str) -> TestConsumer  {
        TestConsumer{
            name: name.to_owned(),
            events: Vec::new(),
        }
    }
}

impl Consumer<String> for TestConsumer {
    fn name(&self) -> &str {
        &self.name
    }

    fn on_event<C: Context<String>>(&mut self, event: Event<String>, _context: &mut C) -> ConsumerAction {
        println!("Consumer {} received event: {:?}", &self.name, event);
        self.events.push(event);
        ConsumerAction::Continue
    }

    fn on_error(&mut self, error: &ClientError) -> ConsumerAction {
        println!("on_error for consumer: {} on event # {} - error: {:?}", self.name, self.events.len() + 1, error);
        if error.is_end_of_stream() {
            ConsumerAction::Continue
        } else {
            ConsumerAction::Stop
        }
    }
}

#[test]
fn consumer_transitions_from_reading_events_from_disk_to_reading_from_memory() {
    use tempdir::TempDir;
    use std::thread;

    init_logger();
    let mut server_process = None;
    let (proc_type, port) = get_server_port();
    if proc_type == ServerProcessType::Child {
        let tempdir = TempDir::new("flo-integraion-test-transitions").unwrap();
        let args = vec!["--max-cached-events".to_owned(), "5".to_owned()];
        server_process = Some(FloServerProcess::with_args(port, tempdir, args));
    }

    let mut producer_connection = SyncConnection::connect(localhost(port), StringCodec).expect("failed to connect");
    let mut consumer_connection = SyncConnection::connect(localhost(port), StringCodec).expect("failed to connect");
    for i in 0..10 {
        producer_connection.produce("/the/namespace", format!("event data: {}", i)).expect("Failed to produce event");
    }

    let join_handle = thread::spawn(move || {
        // Produce one last event asynchronously in another thread.
        thread::sleep(Duration::from_millis(250));
        // After this sleep, the cursor should already be at the end of the first ten events.
        // It _should_ still receive this new event, but it would fail to receive it in the case that the file cursor
        // never notifies the consumer manager that it is exhausted
        producer_connection.produce("/the/namespace", "final event".to_owned()).expect("Failed to produce event");
    });

    let mut consumer = TestConsumer::new("consumer_transitions_from_reading_events_from_disk_to_reading_from_memory");
    let options = ConsumerOptions::new("/the/namespace", VersionVector::new(), 11, true);
    consumer_connection.run_consumer(options, &mut consumer).expect("running consumer failed");

    assert_eq!(11, consumer.events.len());

    join_handle.join().expect("async producer thread resulted in error");

    server_process.map(|mut process| {
        // explicitly kill the server here to ensure that it stays in scope since it's automatically killed on Drop
        process.kill();
    });
}

integration_test!{consumer_responds_to_event, port, _tcp_stream, {
    let _ = ::env_logger::init();
    let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create producer");

    struct RespondingConsumer;
    impl Consumer<String> for RespondingConsumer {
        fn name(&self) -> &str {
            "consumer responds to event"
        }
        fn on_event<C: Context<String>>(&mut self, event: Event<String>, context: &mut C) -> ConsumerAction {
            println!("respondingConsumer got event: {:?}", event);
            let result = context.respond("/responses", event.data);
            println!("Response to the response: {:?}", result);
            result.into()
        }
    }

    let event_1_id = client.produce("/events", "data").unwrap();
    let event_2_id = client.produce("/events", "data 2").unwrap();
    let mut consumer = RespondingConsumer;
    let options = ConsumerOptions::new("/events", VersionVector::new(), 2, true);
    client.run_consumer(options, &mut consumer).expect("failed to run consumer");

    let mut consumer = TestConsumer::new("verify that response events exist");
    let options = ConsumerOptions::new("/responses", VersionVector::new(), 2, true);
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
    let _ = ::env_logger::init();
    let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create producer");

    let mut consumer = TestConsumer::new("consumer_receives_error_after_starting_to_consume_with_invalid_namespace");

    let options = ConsumerOptions::from_beginning("/***", 2);
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
    let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create producer");

    client.produce("/animal/mammal/koala", "data").expect("failed to produce event");
    client.produce("/animal/mammal/sorta/platypus", "data").expect("failed to produce event");
    client.produce("/animal/reptile/snake", "data").expect("failed to produce event");
    client.produce("/animal/mammal/mouse", "data").expect("failed to produce event");
    client.produce("/animal/bird/magpie", "data").expect("failed to produce event");

    let mut consumer = TestConsumer::new("consumer_reads_events_matching_glob_pattern");
    let options = ConsumerOptions::new("/animal/mammal/*", VersionVector::new(), 2, true);
    client.run_consumer(options, &mut consumer).expect("failed to run consumer");

    assert_eq!(2, consumer.events.len());

    assert_eq!("/animal/mammal/koala", consumer.events[0].namespace);
    assert_eq!("/animal/mammal/mouse", consumer.events[1].namespace);
}}

integration_test!{consumer_only_receives_events_with_exactly_matching_namespace, port, tcp_stream, {

    let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create producer");

    let namespace = "/test/namespace";

    client.produce("/wrong/namespace", "wrong data").expect("failed to produce event");
    client.produce("/test/namespace/extra", "wrong data").expect("failed to produce event");
    client.produce(namespace, "right data").expect("failed to produce event");
    client.produce("/wrong/namespace", "wrong data").expect("failed to produce event");
    client.produce(namespace, "right data").expect("failed to produce event");

    let mut consumer = TestConsumer::new("consumer_only_receives_events_with_exactly_matching_namespace");
    let options = ConsumerOptions::new(namespace, VersionVector::new(), 2, true);
    client.run_consumer(options, &mut consumer).expect("failed to run consumer");

    assert_eq!(2, consumer.events.len());

    for event in consumer.events.iter() {
        assert_eq!(namespace, &event.namespace);
    }

}}

integration_test!{clients_can_connect_and_disconnect_multiple_times_without_making_the_server_barf, port, tcp_stream, {

    let namespace = "/the/test/namespace";
    let opts = |start: Option<FloEventId>, max_events: u64| {
        ConsumerOptions::simple(namespace, start.unwrap_or(FloEventId::zero()), max_events)
    };

    {
        let mut client = SyncConnection::from_tcp_stream(tcp_stream, StringCodec);
        client.produce(namespace, "whatever data").expect("failed to produce first event");
    }

    {
        let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create first consumer");
        let mut consumer = TestConsumer::new("consumer one");
        client.run_consumer(opts(None, 1), &mut consumer).expect("failed to run first consumer");
        assert_eq!(1, consumer.events.len());
    }

    let second_event_id = {
        let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create second producer");
        client.produce(namespace, "whatever data").expect("failed to produce second event")
    };

    {
        let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create second consumer");
        let mut consumer = TestConsumer::new("consumer two");
        client.run_consumer(opts(None, 2), &mut consumer).expect("failed to run second consumer");
        assert_eq!(2, consumer.events.len());
    }

    {
        let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create third producer");
        client.produce(namespace, "whatever data").expect("failed to produce third event");
    }

    {
        let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create third consumer");
        let mut consumer = TestConsumer::new("consumer three");
        client.run_consumer(opts(Some(second_event_id), 1), &mut consumer).expect("failed to run third consumer");
        assert_eq!(1, consumer.events.len());
    }
}}

integration_test!{many_events_are_produced_and_read_in_batches, port, tcp_stream, {

    let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to create client");

    for i in 0..1000 {
        match client.produce("/any/ns", "some bytes") {
            Ok(_) => {}
            Err(err) => {
                panic!("Failed to produce event: {}, err: {:?}", i, err);
            }
        }
    }

    client.set_batch_size(100).expect("failed to set batch size");

    let mut consumer = BatchTestConsumer {
        batch_size: 100,
        current_count: 0
    };
    client.run_consumer(ConsumerOptions::from_beginning("/**/*", 1000), &mut consumer).expect("failed to run the BatchConsumer");
}}

pub struct BatchTestConsumer{
    batch_size: u32,
    current_count: u32,
}

impl <T> Consumer<T> for BatchTestConsumer {
    fn name(&self) -> &str {
        "BatchTestConsumer"
    }

    fn on_event<C>(&mut self, event: Event<T>, context: &mut C) -> ConsumerAction where C: Context<T> {
        self.current_count += 1;

        let remaining = context.batch_remaining();

        if remaining > 0 {
            assert_eq!(self.current_count % self.batch_size, self.batch_size - remaining);
        } else {
            assert_eq!(self.current_count % self.batch_size, 0);
        }
        ConsumerAction::Continue
    }
}

integration_test!{events_are_consumed_as_they_are_written, port, tcp_stream, {

    let num_events: usize = 3;

    let join_handle = thread::spawn(move || {
        let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("Failed to create client");

        (0..num_events).map(|i| {
            let event_data = format!("Event: {} data", i);
            client.produce("/the/test/namespace", event_data).expect("Failed to produce event")
        }).collect::<Vec<FloEventId>>()
    });

    let mut consumer = TestConsumer::new("events are consumed as they are written");
    let mut client = SyncConnection::from_tcp_stream(tcp_stream, StringCodec);

    let options = ConsumerOptions::new("/the/test/namespace", VersionVector::new(), num_events as u64, true);
    let result = client.run_consumer(options, &mut consumer);

    let produced_ids = join_handle.join().expect("Producer thread panicked!");
    assert_eq!(num_events, produced_ids.len(), "didn't produce correct number of events");

    assert_eq!(num_events, consumer.events.len(), "didn't consume enough events");

    result.expect("Error running consumer");
}}

integration_test!{event_is_written_using_sync_connection, port, tcp_stream, {
    let mut client = SyncConnection::connect(localhost(port), StringCodec).expect("failed to connect");
    client.produce("/this/namespace/is/boss", "this is the event data").unwrap();
}}
