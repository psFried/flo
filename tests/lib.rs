extern crate flo;
extern crate url;
extern crate env_logger;


use url::Url;
use flo::client::*;
use flo::event::{EventId, Event, Json, ObjectBuilder};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use std::sync::{Once, ONCE_INIT};


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
    pub fn new() -> FloServerProcess {
        use std::path::PathBuf;
        use std::env::current_dir;

        let mut flo_path = current_dir().unwrap();
        flo_path.push("target/debug/flo");
        println!("Starting flo server");
        let child = Command::new(flo_path).spawn().unwrap();

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
            child.kill();
            child.wait();
            println!("flo server proc completed");
        });
    }
}

macro_rules! integration_test {
    ($d:ident, $t:block) => (
        #[test]
        fn $d() {
            init_logger();
            let mut flo_server_proc = FloServerProcess::new();
            $t
        }
    )
}


integration_test!(producer_produces_event_and_gets_event_id_in_response, {
    let url = Url::parse("http://localhost:3000").unwrap();
    let mut producer = FloProducer::new(url);
    let result = producer.emit_raw(r#"{"myKey": "what a great value!"}"#);
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap());
});

integration_test!(producer_emits_multiple_events_and_returns_iterator_of_results, {
    let url = Url::parse("http://localhost:3000").unwrap();
    let mut producer = FloProducer::new(url);

    let events = vec![
        ObjectBuilder::new().insert("keyA", "valueA").unwrap(),
        ObjectBuilder::new().insert("keyB", 123).unwrap(),
        ObjectBuilder::new().insert("keyC", 43.21).unwrap(),
    ];

    let mut results = producer.emit(events.iter());
    let result_a = results.next();
    assert_eq!(Some(ProducerResult::Success(1)), result_a);
    let result_b = results.next();
    assert_eq!(Some(ProducerResult::Success(2)), result_b);
    let result_c = results.next();
    assert_eq!(Some(ProducerResult::Success(3)), result_c);
});
