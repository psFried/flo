extern crate rotor;
extern crate rotor_http;
extern crate netbuf;

mod producer;
mod context;

use rotor::{Scope, Time, Notifier};
use rotor_http::server::{RecvMode, Server, Head, Response, Fsm};
use rotor::mio::tcp::TcpListener;
use context::FloContext;

use std::time::Duration;

#[derive(Debug)]
enum FloServer {
    Producer,
    Consumer
}

impl <'a> Server for FloServer {
    type Seed = ();
    type Context = FloContext;

    fn headers_received(_seed: (),
                        head: Head,
                        res: &mut Response,
                        scope: &mut Scope<FloContext>) -> Option<(Self, RecvMode, Time)>
    {
        // println!("Headers received:\n {:?}", head);
        println!("headers received: {:?} {:?}", head.method, head.path);
        match head.method {
            "GET" => {
                res.status(200u16, "Success");
                res.add_chunked().unwrap();
                res.done_headers().unwrap();

                let notifier = scope.notifier();
                scope.add_consumer(notifier);
                Some((FloServer::Consumer, RecvMode::Buffered(1024), scope.now() + Duration::new(30, 0)))
            },
            "POST" => {
                Some((FloServer::Producer, RecvMode::Buffered(1024), producer::timeout(scope.now())))
            },
            _ => None
        }
    }

    fn request_chunk(self, chunk: &[u8], _response: &mut Response, _scope: &mut Scope<FloContext>) -> Option<Self> {
        unreachable!()
    }

    /// End of request body, only for Progressive requests
    fn request_end(self, _response: &mut Response, _scope: &mut Scope<FloContext>) -> Option<Self> {
        unreachable!()
    }

    fn timeout(self, response: &mut Response, scope: &mut Scope<FloContext>) -> Option<(Self, Time)> {
        println!("** Timeout occured for {:?} ** reponse complete: {:?}", self, response.is_complete());
        match self {
            FloServer::Consumer => Some((self, scope.now() + Duration::new(30, 0))),
            FloServer::Producer => None
        }
    }

    fn wakeup(self, response: &mut Response, scope: &mut Scope<FloContext>) -> Option<Self> {
        if let FloServer::Consumer = self {
            println!("Waking up consumer");
            if let Some(evt) = scope.last_event() {
                println!("writing to consumer: {:?}", evt);
                response.write_body(evt.as_bytes());
            }
        } else {
            println!("waking up producer");
        }
        Some(self)
    }

    fn request_received(self, data: &[u8], res: &mut Response, scope: &mut Scope<FloContext>)
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

fn main() {
    println!("Starting server");
    let event_loop = rotor::Loop::new(&rotor::Config::new()).unwrap();
    let mut loop_inst = event_loop.instantiate(FloContext::new());
    let listener = TcpListener::bind(&"0.0.0.0:3000".parse().unwrap()).unwrap();
    loop_inst.add_machine_with(|scope| {
        Fsm::<FloServer, _>::new(listener, (), scope)
    }).unwrap();
    loop_inst.run().unwrap();
    println!("*** End of main()");
}
