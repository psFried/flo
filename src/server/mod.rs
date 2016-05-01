pub mod consumer;
pub mod producer;


use rotor::{Scope, Time};
use rotor_http::server::{RecvMode, Server, Head, Response, Fsm};
use rotor::mio::tcp::TcpListener;

use context::FloContext;
use self::consumer::RotorConsumerNotifier;
use event_store::FileSystemEventStore;

use std::time::Duration;


#[derive(Debug)]
enum FloServer {
    Producer,
    Consumer(usize)
}

impl <'a> Server for FloServer {
    type Seed = ();
    type Context = FloContext<RotorConsumerNotifier, FileSystemEventStore>;

    fn headers_received(_seed: (),
                        head: Head,
                        res: &mut Response,
                        scope: &mut Scope<Self::Context>) -> Option<(Self, RecvMode, Time)>
    {
        // println!("Headers received:\n {:?}", head);
        println!("headers received: {:?} {:?}", head.method, head.path);
        match head.method {
            "GET" => {
                res.status(200u16, "Success");
                res.add_chunked().unwrap();
                res.done_headers().unwrap();

                let notifier = scope.notifier();
                let consumer_id = scope.add_consumer(RotorConsumerNotifier::new(notifier), 0);

                Some((FloServer::Consumer(consumer_id),
                        RecvMode::Buffered(1024),
                        scope.now() + Duration::new(30, 0)))
            },
            "POST" => {
                Some((FloServer::Producer, RecvMode::Buffered(1024), producer::timeout(scope.now())))
            },
            _ => None
        }
    }

    fn request_chunk(self, _chunk: &[u8], _response: &mut Response, _scope: &mut Scope<Self::Context>) -> Option<Self> {
        unreachable!()
    }

    /// End of request body, only for Progressive requests
    fn request_end(self, _response: &mut Response, _scope: &mut Scope<Self::Context>) -> Option<Self> {
        unreachable!()
    }

    fn timeout(self, response: &mut Response, scope: &mut Scope<Self::Context>) -> Option<(Self, Time)> {
        println!("** Timeout occured for {:?} ** reponse complete: {:?}", self, response.is_complete());
        match self {
            FloServer::Consumer(_idx) => {
                Some((self, scope.now() + Duration::new(30, 0)))
            },
            FloServer::Producer => None
        }
    }

    fn wakeup(self, response: &mut Response, scope: &mut Scope<Self::Context>) -> Option<Self> {
        use serde_json::ser::to_string;
        if let FloServer::Consumer(id) = self {
            println!("Waking up consumer: {}", id);
            if let Some(evt) = scope.last_event() {
                println!("writing to consumer: {:?}", evt);
                let evt_string = to_string(evt).unwrap();
                response.write_body(evt_string.as_bytes());
            }
        } else {
            println!("waking up producer");
        }
        Some(self)
    }

    fn request_received(self, data: &[u8], res: &mut Response, scope: &mut Scope<Self::Context>)
            -> Option<Self>
    {
        if let FloServer::Producer = self {
            producer::handle_request(data, res, scope);
            None
        } else {
            // Consumer won't have anything to do here
            Some(self)
        }
    }
}

pub fn start_server() {
    use std::path::PathBuf;
    use rotor::{Loop, Config};

    println!("Starting server");
    let event_store = FileSystemEventStore::new(PathBuf::from("."));
    let flo_context = FloContext::new(event_store);

    let event_loop = Loop::new(&Config::new()).unwrap();
    let mut loop_inst = event_loop.instantiate(flo_context);
    let listener = TcpListener::bind(&"0.0.0.0:3000".parse().unwrap()).unwrap();
    loop_inst.add_machine_with(|scope| {
        Fsm::<FloServer, _>::new(listener, (), scope)
    }).unwrap();
    loop_inst.run().unwrap();
}
