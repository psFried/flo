use log;
use std::io::Read;
use serde_json::Value;
use event::{Event, EventId};
use rotor_http::client::{
    self,
    connect_tcp,
    Request,
    Head,
    Client,
    RecvMode,
    Connection,
    Requester,
    Task,
    Version,
    ResponseError,
    ProtocolError
};
use rotor::{self, Config, Loop, Scope, Time, Notifier, LoopInstance};
use rotor::mio::tcp::TcpStream;
use url::Url;
use std::time::Duration;
use std::net::ToSocketAddrs;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::Arc;

#[derive(Clone, Debug)]
enum ClientCommand {
    Shutdown,
    Consume(Url)
}

unsafe impl Send for ClientCommand {}

struct FloRequesterContext;

struct FloRotorClient {
    urls: Vec<Url>,
}

#[derive(Debug)]
struct FloRotorRequester {
    url: Url,
}

impl FloRotorRequester {
    pub fn new(url: Url) -> FloRotorRequester {
        FloRotorRequester {
            url: url,
        }
    }
}

impl Client for FloRotorClient {
    type Requester = FloRotorRequester;
    type Seed = ();

    fn create(seed: Self::Seed, _scope: &mut Scope<FloRequesterContext>) -> Self {
        FloRotorClient {
            urls: vec![]
        }
    }

    fn connection_idle(mut self, _conn: &Connection, scope: &mut Scope<FloRequesterContext>) -> Task<FloRotorClient> {
        match self.urls.pop() {
            Some(url) => Task::Request(self, FloRotorRequester::new(url)),
            None => {
                scope.shutdown_loop();
                Task::Close
            }
        }
    }

    fn connection_error(self, err: &ProtocolError, _scope: &mut Scope<FloRequesterContext>) {
        println!("----- Bad response: {} -----", err);
        panic!("oh shit man");
    }

    fn wakeup(self, _connection: &Connection, _scope: &mut Scope<FloRequesterContext>) -> Task<FloRotorClient> {
        println!("Wakeup on Client");
        Task::Close
    }

    fn timeout(self, _connection: &Connection, _scope: &mut Scope<FloRequesterContext>) -> Task<FloRotorClient> {
        println!("Timeout on Client");
        Task::Close
    }
}

impl Requester for FloRotorRequester {
    type Context = FloRequesterContext;

    fn prepare_request(self, req: &mut Request, _scope: &mut Scope<FloRequesterContext>) -> Option<Self> {
        req.start("GET", self.url.path(), Version::Http11);
        req.add_header("User-Agent", b"Flo Client Library");
        req.done_headers().unwrap();
        req.done();
        Some(self)
    }

    fn headers_received(self, head: Head,
            _req: &mut Request,
            scope: &mut Scope<FloRequesterContext>) -> Option<(Self, RecvMode, Time)> {
        None
    }

    fn response_received(self, data: &[u8], req: &mut Request, scope: &mut Scope<FloRequesterContext>) {
        let response_data = String::from_utf8_lossy(data);
        println!("Response received for: {:?}, \ndata: {:?}", self, response_data);
    }

    fn response_chunk(self, chunk: &[u8], req: &mut Request, scope: &mut Scope<FloRequesterContext>) -> Option<Self> {
        let as_str = String::from_utf8_lossy(chunk);
        println!("Chunk: {:?}", as_str);
        Some(self)
    }

    fn response_end(self, req: &mut Request, scope: &mut Scope<FloRequesterContext>) {
        println!("Response ended for: {:?}", self);
    }

    fn timeout(self, req: &mut Request, scope: &mut Scope<FloRequesterContext>) -> Option<(Self, Time)> {
        println!("Timeout for: {:?}", self);
        Some((self, scope.now() + Duration::new(10, 0)))
    }

    fn wakeup(self, req: &mut Request, scope: &mut Scope<FloRequesterContext>) -> Option<Self> {
        println!("Wakeup for: {:?}", self);
        Some(self)
    }

    fn bad_response(self, err: &ResponseError, scope: &mut Scope<FloRequesterContext>) {
        println!("Bad Response for: {:?}", self);
    }
}

pub trait EventListener {
    fn on_event(&mut self, event: Event);
}

struct ClientNotifier {
    sender: Sender<ClientCommand>,
    notifier: Notifier,
}

pub struct FloConsumer {
    event_listener: Box<EventListener>,
    client_notifier: ClientNotifier,
}

/*
fn create_rotor_client(server_url: Url, listener: Box<EventListener>) -> FloConsumer {
    use std::thread;
    use rotor::EarlyScope;


    let (tx, rx) = channel::<ClientCommand>();
    let mut notifier: Option<Notifier> = None;


    let thread_handle = {
        let not = &mut notifier;
        let creator = rotor::Loop::new(&rotor::Config::new()).unwrap();
        creator.add_machine_with(|scope| {
            println!("start of add_machine_with");
            *not = Some(scope.notifier());
            let socket_addr = server_url.to_socket_addrs().unwrap().next().unwrap();
            connect_tcp::<FloRotorClient>(scope, &socket_addr, ())
        }).unwrap();
        let mut loop_inst: LoopInstance<_>  = creator.instantiate(FloRequesterContext);


        thread::spawn(move || {
            println!("About to start client event loop");


            let run_result = loop_inst.run();
            match run_result {
                Ok(_) => println!("Finished running client event loop - OK"),
                Err(e) => println!("Error running client event loop: {:?}", e)
            }
        })
    };

    FloConsumer {
        event_listener: listener,
        client_notifier: notifier.unwrap(),
    }

}
*/
