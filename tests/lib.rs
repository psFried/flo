extern crate flo;
extern crate url;
extern crate env_logger;
extern crate tempdir;


use url::Url;
use flo::client::*;
use flo::event::{EventId, Event, Json, ObjectBuilder};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use std::sync::{Once, ONCE_INIT};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::path::Path;

macro_rules! integration_test {
    ($d:ident, $p:ident, $t:block) => (
        #[test]
        #[allow(unused_variables)]
        fn $d() {
            init_logger();
            let port =  unsafe {
                3000u16 + PORT.fetch_add(1, Ordering::Relaxed) as u16
            };
            let data_dir = tempdir::TempDir::new("flo-integration-test").unwrap();
            let flo_server_proc = FloServerProcess::new(port, data_dir.path());
            let $p: Url = Url::parse(&format!("http://localhost:{}", port)).unwrap();
            $t
        }
    )
}

integration_test!(producer_produces_event_and_gets_event_id_in_response, server_url, {
    let producer = FloProducer::default(server_url);
    let result = producer.emit_raw(r#"{"myKey": "what a great value!"}"#.as_bytes());
    assert_eq!(Ok(1), result);
});

integration_test!(producer_emits_multiple_events_and_returns_iterator_of_results, server_url, {
	    
    let producer = FloProducer::default(server_url);

    let events = vec![
        ObjectBuilder::new().insert("keyA", "valueA").unwrap(),
        ObjectBuilder::new().insert("keyB", 123).unwrap(),
        ObjectBuilder::new().insert("keyC", 43.21).unwrap(),
    ];

	let mut producer_handler = TestProducerHandler::expect_success(vec![1, 2, 3]);
	producer.produce_stream(events.iter(), &mut producer_handler);
});

integration_test!(consumer_consumes_events_starting_at_beginning_of_stream, server_url, {
    let producer = FloProducer::default(server_url.clone());
    let events = vec![
        ObjectBuilder::new().insert("keyA", "valueA").unwrap(),
        ObjectBuilder::new().insert("keyB", 123).unwrap(),
        ObjectBuilder::new().insert("keyC", 43.21).unwrap(),
    ];
	let mut producer_handler = TestProducerHandler::expect_success(vec![1, 2, 3]);
	producer.produce_stream(events.iter(), &mut producer_handler);

    let mut consumer = TestConsumer::new(3);
    let result = run_consumer(&mut consumer, server_url, Duration::from_secs(5));
    assert_eq!(Ok(()), result);
    consumer.assert_event_data_received(&events);
});

integration_test!(consumer_consumes_events_starting_at_the_middle_of_the_stream, server_url, {
    let producer = FloProducer::default(server_url.clone());
	let first_events = vec![
        ObjectBuilder::new().insert("keyA", "valueA").unwrap(),
        ObjectBuilder::new().insert("keyB", 123).unwrap(),
        ObjectBuilder::new().insert("keyC", 43.21).unwrap(),
	];
    let expected_events = vec![
        ObjectBuilder::new().insert("expected1", true).unwrap(),
        ObjectBuilder::new().insert("expected2", "dig it").unwrap(),
        ObjectBuilder::new().insert("expected3", 9999999).unwrap(),
    ];
	let mut producer_handler = TestProducerHandler::expect_success(vec![1, 2, 3, 4, 5, 6]);
	producer.produce_stream(first_events.iter().chain(expected_events.iter()), &mut producer_handler);

    let mut consumer = TestConsumer::new(3);
	let mut consumer_url = server_url.clone();
	consumer_url.query_pairs_mut().append_pair("lastEvent", "3");
    let result = run_consumer(&mut consumer, consumer_url, Duration::from_secs(5));
    assert_eq!(Ok(()), result);
    consumer.assert_event_data_received(&expected_events);
});

integration_test!(consumer_receives_an_event_produced_after_consumer_connected, server_url, {
        
	let event_json = ObjectBuilder::new().insert("brain", "fart").unwrap();
	let expected_events = vec![event_json.clone()];
    let consumer_url = server_url.clone();
    let consumer_thread = thread::spawn(move || {
        let mut consumer = TestConsumer::new(1);
		run_consumer(&mut consumer, consumer_url, Duration::from_secs(5)).unwrap();
		println!("asserting that event json was received");
		consumer.assert_event_data_received(&expected_events);
    });
	
    thread::sleep(Duration::from_millis(500));

	let producer = FloProducer::default(server_url);
	producer.emit(event_json).unwrap();
	println!("Finished emitting event");
	consumer_thread.join().unwrap();
	
});


///////////////////////////////////////////////////////////////////////////
///////  Test Utils            ////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

static mut PORT: AtomicUsize = ATOMIC_USIZE_INIT;

static ON_START: Once = ONCE_INIT;

fn init_logger() {
    ON_START.call_once(|| {
        env_logger::init().unwrap();
    });
}

pub struct FloServerProcess {
    child_proc: Option<Child>,
}

impl FloServerProcess {
    pub fn new(port: u16, data_dir: &Path) -> FloServerProcess {
        use std::env::current_dir;

        let mut flo_path = current_dir().unwrap();
        flo_path.push("target/debug/flo");

        println!("Starting flo server");
        let child = Command::new(flo_path)
                .arg("--port")
                .arg(format!("{}", port))
                .arg("--data-dir")
                .arg(data_dir)
                .spawn().unwrap();

        // hack to prevent test from starting until the server is actually started
        // TODO: wait for log output that indicates server is ready
        thread::sleep(Duration::from_millis(500));
        FloServerProcess {
            child_proc: Some(child)
        }
    }
}

impl Drop for FloServerProcess {
    fn drop(&mut self) {
        self.child_proc.take().map(|mut child| {
            println!("killing flo server proc");
            child.kill().unwrap();
            child.wait().unwrap();
            println!("flo server proc completed");
        });
    }
}


struct TestProducerHandler {
    expected_results: Vec<ProducerResult>,
	current_result: usize,
}

impl TestProducerHandler {
    fn expect_success(event_ids: Vec<EventId>) -> TestProducerHandler {
        let expected_results = event_ids.iter().map(|id| Ok(*id)).collect::<Vec<ProducerResult>>();
        TestProducerHandler {
            expected_results: expected_results,
            current_result: 0usize,
        }
    }
}

impl <'a> StreamProducerHandler<&'a Json> for TestProducerHandler {
    fn handle_result(&mut self, result: ProducerResult, _json: &Json) -> ConsumerCommand {

		assert_eq!(self.expected_results.get(self.current_result), Some(&result));
		self.current_result += 1;
		ConsumerCommand::Continue
    }
}

struct TestConsumer {
    events: Vec<Event>,
    expected_events: usize,
}

impl TestConsumer {
    fn new(expected_events: usize) -> TestConsumer {
        TestConsumer {
            events: Vec::new(),
            expected_events: expected_events,
        }
    }

    fn assert_event_data_received(&self, expected: &Vec<Json>) {
        let num_expected = expected.len();
        let num_received = self.events.len();
        assert!(num_expected == num_received, "Expected {} events, got {}", num_expected, num_received);
        for (actual, expected) in self.events.iter().zip(expected.iter()) {
            let actual_data = actual.data.find("data").expect("Event should have data");
            assert_eq!(actual_data, expected);
        }
    }
}

impl FloConsumer for TestConsumer {
    fn on_event(&mut self, event: Event) -> ConsumerCommand {
        println!("Test Consumer received event: {:?}", event);
        self.events.push(event);

        if self.events.len() < self.expected_events {
            ConsumerCommand::Continue
        } else {
            ConsumerCommand::Stop(Ok(()))
        }
    }
}
