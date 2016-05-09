pub mod consumer;
pub mod producer;


use rotor::{Scope, Time};
use rotor_http::server::{RecvMode, Server, Head, Response, Fsm};
use rotor::mio::tcp::TcpListener;

use context::FloContext;
use self::consumer::{get_last_event_id, RotorConsumerNotifier};
use event_store::FileSystemEventStore;

use std::time::Duration;


#[derive(Debug)]
pub enum FloServer {
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
        match head.method {
            "GET" => {
                self::consumer::init_consumer(head, res, scope)
            },
            "PUT" => {
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
        debug!("** Timeout occured for {:?} ** reponse complete: {:?}", self, response.is_complete());
        match self {
            FloServer::Consumer(_idx) => {
                Some((self, scope.now() + Duration::new(30, 0)))
            },
            FloServer::Producer => None
        }
    }

    fn wakeup(self, response: &mut Response, scope: &mut Scope<Self::Context>) -> Option<Self> {
        if let FloServer::Consumer(id) = self {
            self::consumer::on_wakeup(id, response, scope);
        } else {
            warn!("waking up producer");
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

    info!("Starting server");
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
