extern crate flo;
extern crate flo_event;
extern crate url;
extern crate env_logger;
extern crate tempdir;
extern crate byteorder;

#[macro_use]
extern crate nom;

use flo::client::sync::{SyncConnection, FloConsumer, ConsumerContext, ConsumerAction};
use flo::client::{ConsumerOptions, ClientError};
use flo_event::{FloEventId, OwnedFloEvent, FloEvent};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use std::sync::{Once, ONCE_INIT};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::path::Path;
use std::net::{TcpStream, Shutdown, SocketAddr, SocketAddrV4, IpAddr, Ipv4Addr};
use std::io::{Write, Read};

use byteorder::{ByteOrder, BigEndian};
use tempdir::TempDir;
use nom::{be_u64, be_u32, be_u16};
use url::Url;


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
                flo_proc = Some(FloServerProcess::new(port, data_dir));
                // To give the external process time to start
                thread::sleep(Duration::from_millis(250));
            }


            let address: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));
            let mut $s = TcpStream::connect(address).unwrap();
            $s.set_read_timeout(Some(Duration::from_millis(10000)));

            let $p = port;

            $t

        }
    )
}

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

    fn on_event(&mut self, event_result: Result<OwnedFloEvent, &ClientError>, context: &ConsumerContext) -> ConsumerAction {
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

integration_test!{event_is_written_and_ackgnowledged, _port, tcp_stream, {
    let event_data = b"ninechars";
    let op_id = produce_event(&mut tcp_stream, "/foo/bar", &event_data[..]);

    let mut buff = [0; 1024];
    let nread = tcp_stream.read(&mut buff).unwrap();

    let expected = b"FLO_ACK\n";
    assert_eq!(&expected[..], &buff[..8]);      //header
    let result_op_id = BigEndian::read_u32(&buff[8..12]);
    assert_eq!(op_id, result_op_id);
    assert_eq!(&[0, 0, 0, 0, 0, 0, 0, 1, 0, 1], &buff[12..22]);//event id (actor: u16, event_counter: u64)
}}

integration_test!{persisted_event_are_consumed_after_they_are_written, server_port, tcp_stream, {
    let event1_data = b"first event data";
    let first_namespace = "/first".to_owned();
    produce_event(&mut tcp_stream, &first_namespace, &event1_data[..]);

    let event2_data = b"second event data";
    let second_namespace = "/first".to_owned();
    produce_event(&mut tcp_stream, &second_namespace, &event2_data[..]);

    thread::sleep(Duration::from_millis(250));
    let mut buffer = [0; 128];
    let nread = tcp_stream.read(&mut buffer[..]).unwrap();
    assert!(nread > 0);

    let mut consumer = connect(server_port);
    consumer.write_all(b"FLO_CNS\n").unwrap();
    consumer.write_all(&[0, 0, 0, 0, 0, 0, 0, 2]).unwrap();
    thread::sleep(Duration::from_millis(250));

    let results = read_events(&mut consumer, 2);
    assert_eq!(event1_data, results[0].data());
    assert_eq!(first_namespace, results[0].namespace);
    assert_eq!(event2_data, results[1].data());
    assert_eq!(second_namespace, results[1].namespace);
}}

integration_test!{events_are_consumed_by_another_connection_as_they_are_written, server_port, tcp_stream, {
    let mut consumer = connect(server_port);
    consumer.write_all(b"FLO_CNS\n").unwrap();
    consumer.write_all(&[0, 0, 0, 0, 0, 0, 0, 2]).unwrap();

    let event1_data = b"first event data";
    produce_event(&mut tcp_stream, "/animal/pig", &event1_data[..]);

    let event2_data = b"second event data";
    produce_event(&mut tcp_stream, "/animal/donkey", &event2_data[..]);

    let results = read_events(&mut consumer, 2);
    assert_eq!(event1_data, results[0].data());
    assert_eq!(event2_data, results[1].data());
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
    let stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(Duration::from_millis(1_000))).unwrap();
    stream
}

static mut OP_ID: AtomicUsize = ATOMIC_USIZE_INIT;

fn produce_event(tcp_stream: &mut TcpStream, namespace: &str, bytes: &[u8]) -> u32 {

    let op_id = unsafe {
        OP_ID.fetch_add(1, Ordering::SeqCst) as u32
    };
    tcp_stream.write_all(b"FLO_PRO\n").unwrap();
    tcp_stream.write_all(namespace.as_bytes()).unwrap();
    tcp_stream.write_all(b"\n").unwrap();
    let mut buffer = [0; 4];
    BigEndian::write_u32(&mut buffer[..], op_id);
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
	data_dir: TempDir,
}

impl FloServerProcess {
    pub fn new(port: u16, data_dir: TempDir) -> FloServerProcess {
        let mut server_proc = FloServerProcess {
            child_proc: None,
            port: port,
            data_dir: data_dir
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
                .env("RUST_BACKTRACE", "1")
                .arg("--port")
                .arg(format!("{}", self.port))
                .arg("--data-dir")
                .arg(self.data_dir.path().to_str().unwrap())
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
