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

    fn on_event(&mut self, event: Event) -> ConsumerCommand {
        ConsumerCommand::Stop(Err("oh shit man!".to_string()))
    }

    fn on_timeout(&mut self) -> ConsumerCommand {
        ConsumerCommand::Stop(Err("oh shit man!".to_string()))
    }
}

pub fn start_consumer<T: FloConsumer>(consumer: T, timeout: Duration) -> StopResult {

    Err("oh shit man!".to_string())
}
