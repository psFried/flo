mod client_stream;

use std::io::{self, Write, Read};
use std::time::Duration;
use std::net::{TcpStream, SocketAddr, ToSocketAddrs};

use nom::IResult;

use protocol::{ProtocolMessage, ServerMessage, EventHeader, read_server_message};
use flo_event::{FloEventId, FloEvent, OwnedFloEvent};
use super::{ClientError, ConsumerOptions};

pub use self::client_stream::{SyncStream, ClientStream, IoStream};

pub enum ConsumerAction {
    Continue,
    Stop,
}

pub struct ConsumerContext {
    current_event_id: Option<FloEventId>,
    pub events_consumed: u64,
}

pub trait FloConsumer: Sized {
    fn name(&self) -> &str;
    fn on_event(&mut self, event: Result<OwnedFloEvent, &ClientError>, context: &ConsumerContext) -> ConsumerAction;
}


pub struct SyncConnection<S: IoStream> {
    stream: ClientStream<S>,
}

impl SyncConnection<TcpStream> {
    pub fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<SyncConnection<TcpStream>> {
        SyncStream::connect(addr).map(|stream| {
            SyncConnection {
                stream: stream
            }
        })
    }
}

impl <S: IoStream> SyncConnection<S> {

    pub fn new(stream: ClientStream<S>) -> SyncConnection<S> {
        SyncConnection {
            stream: stream
        }
    }

    pub fn produce<N: ToString, D: AsRef<[u8]>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.stream.produce(namespace, data)
    }

    pub fn run_consumer<C: FloConsumer>(&mut self, options: ConsumerOptions, consumer: &mut C) -> Result<(), ClientError> {
        let ConsumerOptions{namespace, start_position, username, password} = options;

        self.authenticate(namespace, username, password)?;

        let mut context = ConsumerContext {
            current_event_id: None,
            events_consumed: 0
        };

        let mut error: Option<ClientError> = None;

        loop {

            let consumer_action = {
                let read_result = self.read_event();

                let for_consumer = match read_result {
                    Ok(event) => {
                        context.current_event_id = Some(event.id);
                        context.events_consumed += 1;
                        Ok(event)
                    }
                    Err(err) => {
                        error = Some(err);
                        Err(error.as_ref().unwrap())
                    }
                };
                consumer.on_event(for_consumer, &context)
            };

            match consumer_action {
                ConsumerAction::Continue => {
                    context.events_consumed += 1;
                    error = None;
                }
                ConsumerAction::Stop => {
                    debug!("Stopping consumer after error: {:?}", error);
                    break;
                }
            }

        }

        error.map(|err| {
            Err(err)
        }).unwrap_or(Ok(()))
    }

    fn read_event(&mut self) -> Result<OwnedFloEvent, ClientError> {
        match self.stream.read() {
            Ok(ServerMessage::Event(event)) => Ok(event),
            Ok(other) => {
                error!("unexpected message from server: {:?}", other);
                Err(ClientError::UnexpectedMessage(other))
            }
            Err(io_err) => Err(io_err.into())
        }
    }

    fn authenticate(&mut self, namespace: String, username: String, password: String) -> Result<(), ClientError> {
        if username.is_empty() {
            return Ok(())
        }
        let mut auth_msg = ProtocolMessage::ClientAuth{
            username: username,
            password: password,
            namespace: namespace
        };

        self.stream.write(&mut auth_msg).map_err(|io_err| io_err.into())
    }

}
