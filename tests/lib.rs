extern crate flo;
extern crate url;
extern crate env_logger;


use url::Url;
use flo::client::*;
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

#[test]
fn producer_produces_event_and_gets_event_id_in_response() {
    init_logger();
    let server = FloServerProcess::new();
    let url = Url::parse("http://localhost:3000").unwrap();
    let mut producer = FloProducer::new(url);
    let result = producer.emit_raw(r#"{"myKey": "what a great value!"}"#);
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap());
}
