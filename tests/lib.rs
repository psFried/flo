extern crate flo;
extern crate flo_event;
extern crate url;
extern crate env_logger;
extern crate tempdir;
extern crate byteorder;

#[macro_use]
extern crate nom;

use nom::{be_u64, be_u32, be_u16};
use url::Url;
use flo_event::{FloEventId, OwnedFloEvent, FloEvent};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use std::sync::{Once, ONCE_INIT};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::path::Path;
use std::net::{TcpStream, SocketAddr, SocketAddrV4, IpAddr, Ipv4Addr};
use std::io::{Write, Read};
use byteorder::{ByteOrder, BigEndian};

//TODO: get integration tests building again

macro_rules! integration_test {
    ($d:ident, $p:ident, $s:ident, $t:block) => (
        #[test]
        #[allow(unused_variables)]
        fn $d() {
            init_logger();
            let port_var = ::std::env::var("FLO_TEST_PORT").ok().map(|value| {
                value.parse::<u16>().unwrap()
            });

            let port = port_var.unwrap_or_else(|| {
                unsafe {
                    3001u16 + PORT.fetch_add(1, Ordering::Relaxed) as u16
                }
            });

            let mut flo_proc: Option<FloServerProcess> = None;
            if port_var.is_none() {
                // if env variable is not defined, then we need to start the server process ourselves
                let data_dir = tempdir::TempDir::new("flo-integration-test").unwrap();
                flo_proc = Some(FloServerProcess::new(port, data_dir.path()));
                // To give the external process time to start
                thread::sleep(Duration::from_millis(250));
            }


            let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));
            let mut $s = TcpStream::connect(address).unwrap();
            $s.set_read_timeout(Some(Duration::from_millis(1000)));

            let $p = port;

            $t
        }
    )
}

integration_test!{event_is_written_and_ackgnowledged, _port, tcp_stream, {
    let event_data = b"ninechars";
    let op_id = produce_event(&mut tcp_stream, &event_data[..]);

    let mut buff = [0; 1024];
    let nread = tcp_stream.read(&mut buff).unwrap();

    let expected = b"FLO_ACK\n";
    assert_eq!(&expected[..], &buff[..8]);      //header
    let result_op_id = BigEndian::read_u32(&buff[8..12]);
    assert_eq!(op_id, result_op_id);
    assert_eq!(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 1], &buff[12..22]);//event id (actor: u16, event_counter: u64)
}}

integration_test!{persisted_event_are_consumed_after_they_are_written, server_port, tcp_stream, {
    let event1_data = b"first event data";
    produce_event(&mut tcp_stream, &event1_data[..]);

    let event2_data = b"second event data";
    produce_event(&mut tcp_stream, &event2_data[..]);

    thread::sleep(Duration::from_millis(250));
    let mut buffer = [0; 128];
    let nread = tcp_stream.read(&mut buffer[..]).unwrap();
    assert!(nread > 0);

    let mut consumer = connect(server_port);
    consumer.write_all(b"FLO_CNS\n").unwrap();
    consumer.write_all(&[0, 0, 0, 0, 0, 0, 0, 2]).unwrap();
    thread::sleep(Duration::from_millis(250));

    let results = read_events(&mut consumer, 2);
}}


///////////////////////////////////////////////////////////////////////////
///////  Test Utils            ////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

named!{pub parse_str<String>,
    map_res!(
        take_until_and_consume!("\n"),
        |res| {
            ::std::str::from_utf8(res).map(|val| val.to_owned())
        }
    )
}

named!{parse_event<OwnedFloEvent>,
    chain!(
        _tag: tag!("FLO_EVT\n") ~
        actor: be_u16 ~
        counter: be_u64 ~
        namespace: parse_str ~
        data: length_bytes!(be_u32),
        || {
            OwnedFloEvent {
                id: FloEventId::new(actor, counter),
                namespace: namespace.to_owned(),
                data: data.to_owned()
            }
        }
    )
}

fn read_events(tcp_stream: &mut TcpStream, mut nevents: usize) -> Vec<OwnedFloEvent> {
    use nom::IResult;

    tcp_stream.set_read_timeout(Some(Duration::from_millis(1000)));
    let mut events = Vec::new();
    let mut buffer_array = [0; 8 * 1024];
    let mut nread = tcp_stream.read(&mut buffer_array[..]).unwrap();
    let mut buffer_start = 0;
    let mut buffer_end = nread;
    while nevents > 0 {
        if buffer_start == buffer_end {
            println!("reading again");
            nread = tcp_stream.read(&mut buffer_array[..]).unwrap();
            buffer_end = nread;
            buffer_start = 0;
        }
        let mut buffer = &mut buffer_array[buffer_start..buffer_end];
        println!("attempting to read event: {}, buffer: {:?}", nevents, buffer);
        match parse_event(buffer) {
            IResult::Done(mut remaining, event) => {
                events.push(event);
                buffer_start += buffer.len() - remaining.len();
            }
            IResult::Error(err) => panic!("Error deserializing event: {:?}", err),
            IResult::Incomplete(need) => {
                panic!("Incomplete data to read events: {:?}, buffer: {:?}", need, buffer)
            }
        };
        nevents -= 1;
    }
    events
}

fn connect(port: u16) -> TcpStream {
    let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(Duration::from_millis(1_000)));
    stream
}

static mut OP_ID: AtomicUsize = ATOMIC_USIZE_INIT;

fn produce_event(tcp_stream: &mut TcpStream, bytes: &[u8]) -> u32 {

    let op_id = unsafe {
        OP_ID.fetch_add(1, Ordering::SeqCst) as u32
    };
    let mut buffer = [0; 4];
    BigEndian::write_u32(&mut buffer[..], op_id);
    tcp_stream.write_all(b"FLO_PRO\n").unwrap();
    tcp_stream.write_all(&buffer).unwrap();

    BigEndian::write_u32(&mut buffer[..], bytes.len() as u32);
    tcp_stream.write_all(&buffer).unwrap();
    tcp_stream.write_all(bytes).unwrap();

    op_id
}

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
