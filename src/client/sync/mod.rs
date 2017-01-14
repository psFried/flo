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

    pub fn from_tcp_stream(tcp_stream: TcpStream) -> SyncConnection<TcpStream> {
        SyncConnection {
            stream: SyncStream::from_stream(tcp_stream)
        }
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
        let ConsumerOptions{namespace, start_position, username, password, max_events} = options;

        self.authenticate(namespace, username, password)?;
        if let Some(id) = start_position {
            self.send_event_marker(id)?;
        }
        self.start_consuming(max_events)?;

        let mut context = ConsumerContext {
            current_event_id: None,
            events_consumed: 0
        };

        let mut error: Option<ClientError> = None;

        while context.events_consumed < max_events {

            let consumer_action = {
                let read_result = self.read_event();

                let for_consumer = match read_result {
                    Ok(event) => {
                        trace!("Client '{}' received event: {:?}", consumer.name(), event.id);
                        context.current_event_id = Some(event.id);
                        context.events_consumed += 1;
                        Ok(event)
                    }
                    Err(err) => {
                        error!("Consumer: '{}' - Error reading event: {:?}", consumer.name(), err);
                        error = Some(err);
                        Err(error.as_ref().unwrap())
                    }
                };
                consumer.on_event(for_consumer, &context)
            };

            match consumer_action {
                ConsumerAction::Continue => {
                    error.take().map(|err| {
                        info!("Consumer: '{}' - Continuing after error: {:?}", consumer.name(), err);
                    });
                }
                ConsumerAction::Stop => {
                    debug!("Stopping consumer '{}' after error: {:?}", consumer.name(), error);
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
            Ok(other) => Err(ClientError::UnexpectedMessage(other)),
            Err(io_err) => Err(io_err.into())
        }
    }

    fn start_consuming(&mut self, max: u64) -> Result<(), ClientError> {
        let mut msg = ProtocolMessage::StartConsuming(max as i64);
        self.stream.write(&mut msg).map_err(|e| e.into())
    }

    fn send_event_marker(&mut self, id: FloEventId) -> Result<(), ClientError> {
        let mut msg = ProtocolMessage::UpdateMarker(id);
        self.stream.write(&mut msg).map_err(|e| e.into())
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
