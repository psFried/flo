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
use std::thread::JoinHandle;
use event::{self, Json};
use std::marker::PhantomData;

#[derive(Clone, Debug)]
enum ClientCommand {
    Shutdown,
    Consume(Url),
    Produce(Json),
}
unsafe impl Send for ClientCommand {}



struct FloRotorClient {
    server_url: Url,
}



impl Client for FloRotorClient {
    type Requester = FloRotorRequester;
    type Seed = Url;

    fn create(seed: Self::Seed, _scope: &mut Scope<ProducerContext>) -> Self {
        FloRotorClient {
            server_url: seed,
        }
    }

    fn connection_idle(mut self, _conn: &Connection, scope: &mut Scope<ProducerContext>) -> Task<FloRotorClient> {
        println!("Connection-Idle");
        match scope.receiver.recv() {
            Ok(command) => {
                println!("got command: {:?}", command);
                match command {
                    ClientCommand::Produce(json) => Task::Request(self, FloRotorRequester::Producer(json)),
                    _ => {
                        scope.shutdown_loop();
                        Task::Close
                    }
                }
            },
            Err(e) => {
                println!("client received error: {:?}", e);
                scope.shutdown_loop();
                Task::Close
            }
        }
    }

    fn connection_error(self, err: &ProtocolError, scope: &mut Scope<ProducerContext>) {
        println!("----- Client Connection Error: {} -----", err);
        scope.shutdown_loop();
    }

    fn wakeup(self, _connection: &Connection, scope: &mut Scope<ProducerContext>) -> Task<FloRotorClient> {
        println!("Wakeup on Client");
        scope.shutdown_loop();
        Task::Close
    }

    fn timeout(self, _connection: &Connection, scope: &mut Scope<ProducerContext>) -> Task<FloRotorClient> {
        println!("Timeout on Client");
        scope.shutdown_loop();
        Task::Close
    }
}


#[derive(Debug)]
enum FloRotorRequester {
    Producer(Json),
}

impl Requester for FloRotorRequester {
    type Context = ProducerContext;

    fn prepare_request(self, req: &mut Request, scope: &mut Scope<ProducerContext>) -> Option<Self> {
        match self {
            FloRotorRequester::Producer(ref json) => {
                let body = event::to_bytes(json).unwrap();

                req.start("PUT", scope.url.path(), Version::Http11);
                req.add_header("User-Agent", b"Flo Client Library");
                req.add_length(body.len() as u64);
                req.done_headers().unwrap();
                req.write_body(&body);
                req.done();
            }
        }
        Some(self)
    }

    fn headers_received(self, head: Head,
            _req: &mut Request,
            scope: &mut Scope<ProducerContext>) -> Option<(Self, RecvMode, Time)> {
        debug!("Received headers");
        Some((self, RecvMode::Buffered(1024), scope.now() + Duration::new(10, 0)))
    }

    fn response_received(self, data: &[u8], req: &mut Request, scope: &mut Scope<ProducerContext>) {
        use serde_json::from_slice;

        let body = String::from_utf8_lossy(data);
        debug!("got response: {:?}", body);

        from_slice(data).map_err(|serde_err| {
            format!("unable to parse response - {:?}", serde_err)
        }).and_then(|json: Json| {
            json.find("id").and_then(|value: &Json| {
                value.as_u64()
            }).ok_or("Response did not include event id".to_string())
        }).map(|event_id| {
            debug!("sending success response with id: {}", event_id);
            scope.sender.send(ProducerResult::Success(event_id));
        }).map_err(|err| {
            warn!("sending error response with value: {:?}", &err);
            scope.sender.send(ProducerResult::Error(err));
        });
    }

    fn response_chunk(self, chunk: &[u8], req: &mut Request, scope: &mut Scope<ProducerContext>) -> Option<Self> {
        let as_str = String::from_utf8_lossy(chunk);
        println!("Chunk: {:?}", as_str);
        Some(self)
    }

    fn response_end(self, req: &mut Request, scope: &mut Scope<ProducerContext>) {
        println!("Response ended for: {:?}", self);
    }

    fn timeout(self, req: &mut Request, scope: &mut Scope<ProducerContext>) -> Option<(Self, Time)> {
        println!("Timeout for: {:?}", self);
        Some((self, scope.now() + Duration::new(10, 0)))
    }

    fn wakeup(self, req: &mut Request, scope: &mut Scope<ProducerContext>) -> Option<Self> {
        println!("Wakeup for: {:?}", self);
        Some(self)
    }

    fn bad_response(self, err: &ResponseError, scope: &mut Scope<ProducerContext>) {
        println!("Bad Response: {:?}", err);
    }
}

enum ProducerResult {
    Success(EventId),
    Error(String),
    Closed,
}

struct ProducerContext {
    receiver: Receiver<ClientCommand>,
    sender: Sender<ProducerResult>,
    url: Url,
}

#[derive(Debug, Clone)]
pub enum ProducerError {
    InvalidJson,
    ProducerShutdown,
    Wtf(String),
}

pub struct FloProducer {
    sender: Sender<ClientCommand>,
    receiver: Receiver<ProducerResult>,
    thread_handle: Option<JoinHandle<()>>,
}

impl FloProducer {

    pub fn new(server_url: Url) -> FloProducer {
        use std::thread;
        use std::net::SocketAddr;

        let (producer_tx, loop_rx) = channel::<ClientCommand>();
        let (loop_tx, producer_rx) = channel::<ProducerResult>();

        let address = server_url.to_socket_addrs().unwrap().filter(|addr| {
            match addr {
                &SocketAddr::V4(_) => true,
                _ => false
            }
        }).next().expect("Could not fine an IpV4 address");
        debug!("using address: {:?}", address);

        let thread_handle = thread::spawn(move || {
            debug!("starting producer loop");
            let context = ProducerContext {
                receiver: loop_rx,
                sender: loop_tx,
                url: server_url.clone(),
            };
            let loop_creater = rotor::Loop::new(&rotor::Config::new()).unwrap();
            let mut loop_instance = loop_creater.instantiate(context);
            loop_instance.add_machine_with(move |scope| {
                connect_tcp::<FloRotorClient>(scope, &address, server_url)
            });
            loop_instance.run();
            debug!("End of event loop thread");
        });

        FloProducer {
            sender: producer_tx,
            receiver: producer_rx,
            thread_handle: Some(thread_handle),
        }
    }

    pub fn emit_raw(&self, json_str: &str) -> Result<EventId, ProducerError> {
        use serde_json::from_slice;

        if let Ok(parsed_json) = from_slice(json_str.as_bytes()) {
            self.sender.send(ClientCommand::Produce(parsed_json));

            self.receiver.recv().map_err(|recv_err| {
                ProducerError::ProducerShutdown
            }).and_then(|producer_result| {
                match producer_result {
                    ProducerResult::Success(event_id) => Ok(event_id),
                    ProducerResult::Error(message) => Err(ProducerError::Wtf(message)),
                    ProducerResult::Closed => Err(ProducerError::ProducerShutdown),
                }
            })
        } else {
            Err(ProducerError::InvalidJson)
        }
    }

    pub fn shutdown(&mut self) {
        trace!("shutdown called");
        self.sender.send(ClientCommand::Shutdown);
        self.thread_handle.take().map(|join_handle| {
            println!("Shutting down producer");
            match join_handle.join() {
                Ok(_) => debug!("Successfully shutdown producer"),
                Err(err) => error!("Error shutting down producer - {:?}", err)
            }
        });
    }
}

impl Drop for FloProducer {
    fn drop(&mut self) {
        trace!("Dropping producer");
        self.shutdown();
        trace!("Finished Dropping producer");
    }
}
