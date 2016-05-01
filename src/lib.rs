extern crate rotor;
extern crate rotor_http;
extern crate netbuf;
extern crate serde_json;

#[cfg(test)]
extern crate httparse;

#[cfg(test)]
extern crate tempdir;

mod consumer;
mod producer;
mod context;
mod event_store;

#[cfg(test)]
mod test_utils;

use rotor::{Scope, Time, Notifier};
use rotor_http::server::{RecvMode, Server, Head, Response, Fsm};
use rotor::mio::tcp::TcpListener;
use rotor::mio::{EventLoop, Handler};

use context::FloContext;
use consumer::{FloConsumer, FloRotorConsumer};
use event_store::{EventStore, FileSystemEventStore};

use std::time::Duration;

pub type Event = serde_json::Value;
pub type ParseResult = Result<serde_json::Value, serde_json::Error>;

pub fn to_event(json: &str) -> ParseResult {
    serde_json::from_str(json)
}

#[derive(Debug)]
enum FloServer {
    Producer,
    Consumer
}

impl <'a> Server for FloServer {
    type Seed = ();
    type Context = FloContext<FloRotorConsumer, FileSystemEventStore>;

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
                scope.add_consumer(FloRotorConsumer::new(notifier));
                Some((FloServer::Consumer, RecvMode::Buffered(1024), scope.now() + Duration::new(30, 0)))
            },
            "POST" => {
                Some((FloServer::Producer, RecvMode::Buffered(1024), producer::timeout(scope.now())))
            },
            _ => None
        }
    }

    fn request_chunk(self, chunk: &[u8], _response: &mut Response, _scope: &mut Scope<Self::Context>) -> Option<Self> {
        unreachable!()
    }

    /// End of request body, only for Progressive requests
    fn request_end(self, _response: &mut Response, _scope: &mut Scope<Self::Context>) -> Option<Self> {
        unreachable!()
    }

    fn timeout(self, response: &mut Response, scope: &mut Scope<Self::Context>) -> Option<(Self, Time)> {
        println!("** Timeout occured for {:?} ** reponse complete: {:?}", self, response.is_complete());
        match self {
            FloServer::Consumer => Some((self, scope.now() + Duration::new(30, 0))),
            FloServer::Producer => None
        }
    }

    fn wakeup(self, response: &mut Response, scope: &mut Scope<Self::Context>) -> Option<Self> {
        use serde_json::ser::to_string;
        if let FloServer::Consumer = self {
            println!("Waking up consumer");
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
    
    println!("Starting server");
    let mut event_store = FileSystemEventStore::new(PathBuf::from("."));
    let mut flo_context = FloContext::new(event_store);

    let event_loop = rotor::Loop::new(&rotor::Config::new()).unwrap();
    let mut loop_inst = event_loop.instantiate(flo_context);
    let listener = TcpListener::bind(&"0.0.0.0:3000".parse().unwrap()).unwrap();
    loop_inst.add_machine_with(|scope| {
        Fsm::<FloServer, _>::new(listener, (), scope)
    }).unwrap();
    loop_inst.run().unwrap();
}
