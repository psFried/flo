extern crate flo;
extern crate url;
extern crate env_logger;
extern crate tempdir;


use url::Url;
use flo::event::{EventId, Event};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use std::sync::{Once, ONCE_INIT};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::path::Path;
use std::net::{TcpStream, SocketAddr, SocketAddrV4, IpAddr, Ipv4Addr};
use std::io::{Write, Read};

//TODO: get integration tests building again

macro_rules! integration_test {
    ($d:ident, $p:ident, $t:block) => (
        #[test]
        #[allow(unused_variables)]
        fn $d() {
            init_logger();
            let port_var = ::std::env::var("FLO_TEST_PORT").map(|value| {
                value.parse::<u16>().unwrap()
            });

            if let Ok(port) = port_var {
                let $p = port;
                $t
            } else {
                let $p = unsafe {
                    3000u16 + PORT.fetch_add(1, Ordering::Relaxed) as u16
                };
                let data_dir = tempdir::TempDir::new("flo-integration-test").unwrap();
                let flo_server_proc = FloServerProcess::new($p, data_dir.path());
                $t
            }
        }
    )
}

integration_test!{event_is_written_and_ackgnowledged, server_port, {
    let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), server_port));
    let mut tcp_stream = TcpStream::connect(address).unwrap();

    tcp_stream.write_all(b"FLO_PRO\n").unwrap();
    tcp_stream.write_all(&[0, 1, 2, 3]).unwrap();
    tcp_stream.write_all(&[0, 0, 0, 9]).unwrap();
    tcp_stream.write_all(b"ninechars").unwrap();

    let mut buff = [0; 1024];
    let nread = tcp_stream.read(&mut buff).unwrap();

    println!("read {} bytes: {:?}", nread, &buff[..nread]);

    let expected = b"FLO_ACK\n";
    assert_eq!(&expected[..], &buff[..8]);      //header
    assert_eq!(&[0, 1, 2, 3], &buff[8..12]);    //op_id
    assert_eq!(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 1], &buff[12..22]);//event id (actor: u16, event_counter: u64)

}}


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
	port: u16,
	data_dir: String,
}

impl FloServerProcess {
    pub fn new(port: u16, data_dir: &Path) -> FloServerProcess {
        let mut server_proc = FloServerProcess {
            child_proc: None,
            port: port,
            data_dir: data_dir.to_str().unwrap().to_string()
        };
        server_proc.start();
        server_proc
    }

	pub fn start(&mut self) {
        use std::env::current_dir;
		use std::process::Stdio;
		
		assert!(self.child_proc.is_none(), "tried to start server but it's already started");

        let mut flo_path = current_dir().unwrap();
        flo_path.push("target/debug/flo");

        println!("Starting flo server");
        let child = Command::new(flo_path)
                .arg("--port")
                .arg(format!("{}", self.port))
                .arg("--data-dir")
                .arg(&self.data_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn().unwrap();
		self.child_proc = Some(child);

        // hack to prevent test from starting until the server is actually started
        // TODO: wait for log output that indicates server is ready
        thread::sleep(Duration::from_millis(500));
	}

	pub fn kill(&mut self) {
        self.child_proc.take().map(|mut child| {
            println!("killing flo server proc");
            child.kill().unwrap();
            child.wait_with_output().map(|output| {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    println!("stdout: \n {}", stdout);
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    println!("stderr: \n {}", stderr);
            }).unwrap();
            println!("flo server proc completed");
        });
	}
}

impl Drop for FloServerProcess {
    fn drop(&mut self) {
		self.kill();
    }
}

/*

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
*/
