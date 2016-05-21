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

pub type StopResult = Result<(), String>;

pub enum ConsumerCommand {
    Continue,
    Stop(StopResult)
}

pub trait FloConsumer {

    fn get_event_criteria(&self) -> EventId {
        0
    }

    fn on_event(&mut self, event: Event) -> ConsumerCommand;

    fn on_timeout(&mut self) -> ConsumerCommand {
        ConsumerCommand::Stop(Err("Consumer timed out".to_string()))
    }
}

pub fn run_consumer<T: FloConsumer>(consumer: &mut T, server_url: Url, timeout: Duration) -> StopResult {
    use std::thread;
    use std::sync::mpsc::channel;
    use std::net::SocketAddr;

    let (client_tx, consumer_rx) = channel::<ClientResponse>();
    let (consumer_tx, client_rx) = channel::<ClientCommand>();
    let starting_event = consumer.get_event_criteria();

    let join_handle = thread::spawn(move || {
        let address = server_url.to_socket_addrs().unwrap().filter(|addr| {
            match addr {
                &SocketAddr::V4(_) => true,
                _ => false
            }
        }).next().expect("Could not find an IpV4 address");
        debug!("Consumer using address: {:?}", address);

        let context = ConsumerContext {
            sender: client_tx,
            receiver: client_rx,
            server_url: server_url,
        };

        let loop_creater = rotor::Loop::new(&rotor::Config::new()).unwrap();
        let mut loop_instance = loop_creater.instantiate(context);
        loop_instance.add_machine_with(move |scope| {
            connect_tcp::<FloConsumerClient>(scope, &address, ())
        });
        loop_instance.run();
        debug!("End of Consumer loop thread");
    });

    let mut event_processor = ConsumerEventIter {
        sender: consumer_tx,
        receiver: consumer_rx,
        consumer: consumer,
    };

    event_processor.run()
}


struct ConsumerEventIter<'a, T: FloConsumer + 'a> {
    sender: Sender<ClientCommand>,
    receiver: Receiver<ClientResponse>,
    consumer: &'a mut T
}

impl <'a, T: FloConsumer> ConsumerEventIter<'a, T> {

    fn run(self) -> StopResult {
        let ConsumerEventIter{sender, receiver, consumer} = self;
        loop {
            match receiver.recv() {
                Ok(Ok(event)) => {
                    let consumer_result = consumer.on_event(event);
                },
                Ok(Err(err)) => error!("Received Error {:?}", err),
                Err(err) => error!("Error reading from channel: {:?}", err),
            }
            sender.send(ClientCommand::Stop);
            return Err("i just never finished this fucking code".to_string());
        }
    }
}


enum ClientCommand {
    Get(EventId),
    Stop,
}

type ClientResponse = Result<Event, ClientErr>;

#[derive(Debug)]
enum ClientErr {
    Timeout,
    Other(String),
}

type ConsumerResult = Result<Event, String>;

struct ConsumerContext {
    sender: Sender<ClientResponse>,
    receiver: Receiver<ClientCommand>,
    server_url: Url,
}

#[derive(Debug, PartialEq)]
struct FloConsumerClient;

impl Client for FloConsumerClient {
    type Requester = ConsumerRequester;
    type Seed = ();

    fn create(seed: Self::Seed, _scope: &mut Scope<ConsumerContext>) -> Self {
        FloConsumerClient
    }

    fn connection_idle(mut self, _conn: &Connection, scope: &mut Scope<ConsumerContext>) -> Task<FloConsumerClient> {
        trace!("Connection-Idle on Consumer Client");
        Task::Request(self, ConsumerRequester::new())
    }

    fn connection_error(self, err: &ProtocolError, scope: &mut Scope<ConsumerContext>) {
        error!("----- Client Connection Error: {} -----", err);
        scope.shutdown_loop();
    }

    fn wakeup(self, _connection: &Connection, scope: &mut Scope<ConsumerContext>) -> Task<FloConsumerClient> {
        error!("Wakeup on Client");
        scope.shutdown_loop();
        Task::Close
    }

    fn timeout(self, _connection: &Connection, scope: &mut Scope<ConsumerContext>) -> Task<FloConsumerClient> {
        debug!("Timeout on Consumer Client");
        scope.sender.send(Err(ClientErr::Timeout))
                .map_err(|err| error!("Error sending connection timeout to consumer"));
        scope.shutdown_loop();
        Task::Close
    }
}

struct ConsumerRequester {
    buffer: Vec<u8>,
}

impl ConsumerRequester {
    pub fn new() -> ConsumerRequester {
        ConsumerRequester {
            buffer: Vec::new(),
        }
    }
}

impl Requester for ConsumerRequester {
    type Context = ConsumerContext;

    fn prepare_request(self, req: &mut Request, scope: &mut Scope<ConsumerContext>) -> Option<Self> {
        trace!("Preparing request for consumer");
        req.start("GET", scope.server_url.path(), Version::Http11);
        req.add_header("User-Agent", b"Flo Client Library");
        req.done_headers().unwrap();
        req.done();
        Some(self)
    }

    fn headers_received(self, head: Head, req: &mut Request, scope: &mut Scope<ConsumerContext>) -> Option<(Self, RecvMode, Time)> {
        debug!("Response Status: {} - {}", head.code, head.reason);
        if head.code == 200 {
            Some((self, RecvMode::Progressive(1024), scope.now() + Duration::new(10, 0)))
        } else {
            None
        }
    }

    fn response_received(self, data: &[u8], request: &mut Request, scope: &mut Scope<Self::Context>) {
        unreachable!()
    }

    fn response_chunk(self, chunk: &[u8], request: &mut Request, scope: &mut Scope<Self::Context>) -> Option<Self> {
        match Event::from_slice(chunk) {
            Ok(event) => {
                trace!("Received event: {:?}", event);
                scope.sender.send(Ok(event)).map_err(|send_err| {
                    error!("Error sending Event to consumer: {:?}", send_err);
                }).map(|_| Some(self)).ok().unwrap_or(None)
            },
            Err(deser_err) => {
                error!("Error deserializing - err: {:?},\n chunk: {}", deser_err, String::from_utf8_lossy(chunk));
                scope.sender.send(Err(ClientErr::Other(format!("Error deserializing response: {}", deser_err))));
                None
            }
        }

    }

    fn response_end(self, request: &mut Request, scope: &mut Scope<Self::Context>) {

    }

    fn timeout(self, request: &mut Request, scope: &mut Scope<Self::Context>) -> Option<(Self, Time)> {
        None
    }

    fn wakeup(self, request: &mut Request, scope: &mut Scope<Self::Context>) -> Option<Self> {
        None
    }

}
