use log;
use std::io::Read;
use serde_json::Value;
use event::{self, Event, EventId, Json};
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
use std::marker::PhantomData;

#[derive(Clone, Debug)]
enum ClientCommand {
    Shutdown,
    Produce(Vec<u8>),
}
unsafe impl Send for ClientCommand {}



#[derive(Debug, PartialEq)]
struct FloRotorClient {
    server_url: Url,
}



impl Client for FloRotorClient {
    type Requester = ProducerRequester;
    type Seed = Url;

    fn create(seed: Self::Seed, _scope: &mut Scope<ProducerContext>) -> Self {
        FloRotorClient {
            server_url: seed,
        }
    }

    fn connection_idle(mut self, _conn: &Connection, scope: &mut Scope<ProducerContext>) -> Task<FloRotorClient> {
        trace!("Connection-Idle - {:?}", self);
        match scope.receiver.recv() {
            Ok(command) => {
                match command {
                    ClientCommand::Produce(json) => {
                        trace!("{:?} received Produce command", self);
                        Task::Request(self, ProducerRequester::new(json))
                    },
                    _ => {
                        scope.shutdown_loop();
                        Task::Close
                    }
                }
            },
            Err(e) => {
                error!("client receiver error: {:?}", e);
                scope.shutdown_loop();
                Task::Close
            }
        }
    }

    fn connection_error(self, err: &ProtocolError, scope: &mut Scope<ProducerContext>) {
        error!("----- Client Connection Error: {} -----", err);
        scope.shutdown_loop();
    }

    fn wakeup(self, _connection: &Connection, scope: &mut Scope<ProducerContext>) -> Task<FloRotorClient> {
        error!("Wakeup on Client");
        scope.shutdown_loop();
        Task::Close
    }

    fn timeout(self, _connection: &Connection, scope: &mut Scope<ProducerContext>) -> Task<FloRotorClient> {
        debug!("Timeout on Client");
        scope.shutdown_loop();
        Task::Close
    }
}


#[derive(Debug)]
pub struct ProducerRequester {
    data: Vec<u8>,
}

impl Requester for ProducerRequester {
    type Context = ProducerContext;

    fn prepare_request(self, req: &mut Request, scope: &mut Scope<ProducerContext>) -> Option<Self> {
        {
            let json = &self.data;
            req.start("PUT", scope.url.path(), Version::Http11);
            req.add_header("User-Agent", b"Flo Client Library");
            req.add_length(json.len() as u64);
            req.done_headers().unwrap();
            req.write_body(json);
            req.done();
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
		unreachable!();
    }

    fn response_end(self, req: &mut Request, scope: &mut Scope<ProducerContext>) {
        debug!("Response ended for: {:?}", self);
    }

    fn timeout(self, req: &mut Request, scope: &mut Scope<ProducerContext>) -> Option<(Self, Time)> {
        debug!("Timeout for: {:?}", self);
        Some((self, scope.now() + Duration::new(10, 0)))
    }

    fn wakeup(self, req: &mut Request, scope: &mut Scope<ProducerContext>) -> Option<Self> {
		unreachable!();
    }

    fn bad_response(self, err: &ResponseError, scope: &mut Scope<ProducerContext>) {
        error!("Bad Response: {:?}", err);
    }
}

impl ProducerRequester {

    pub fn new(data: Vec<u8>) -> ProducerRequester {
        ProducerRequester {
            data: data
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ProducerResult {
    Success(EventId),
    Error(String),
    Closed,
}

pub struct ProducerContext {
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

pub struct ProducerResults<'a> {
    receiver: &'a Receiver<ProducerResult>,
}

impl <'a> Iterator for ProducerResults<'a> {
    type Item=ProducerResult;

    fn next(&mut self) -> Option<ProducerResult> {
        self.receiver.recv().ok()
    }
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

    pub fn emit<'a, 'b, T>(&'a self, events: T) -> ProducerResults<'a>
            where T: Iterator<Item=&'b Json> + Sized {

        for event in events {
            let event_bytes = event::to_bytes(event).unwrap();
            self.sender.send(ClientCommand::Produce(event_bytes));
        }

        ProducerResults {
            receiver: &self.receiver
        }

    }

    pub fn emit_raw<T: Into<Vec<u8>>>(&self, json_str: T) -> Result<EventId, ProducerError> {
        use serde_json::from_slice;

        self.sender.send(ClientCommand::Produce(json_str.into()));

        self.receiver.recv().map_err(|recv_err| {
            ProducerError::ProducerShutdown
        }).and_then(|producer_result| {
            match producer_result {
                ProducerResult::Success(event_id) => Ok(event_id),
                ProducerResult::Error(message) => Err(ProducerError::Wtf(message)),
                ProducerResult::Closed => Err(ProducerError::ProducerShutdown),
            }
        })
    }

    pub fn shutdown(&mut self) {
        trace!("shutdown called");
        self.sender.send(ClientCommand::Shutdown);
        self.thread_handle.take().map(|join_handle| {
            info!("Shutting down producer");
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
