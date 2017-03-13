mod client_stream;
mod error;

use std::io;
use std::net::{TcpStream, ToSocketAddrs};

use protocol::{ProtocolMessage, ProduceEvent, ConsumerStart, ErrorMessage};
use event::{FloEventId, OwnedFloEvent};
use std::collections::VecDeque;

pub use self::error::ClientError;
pub use self::client_stream::{SyncStream, ClientStream, IoStream};

#[derive(Debug, PartialEq, Clone)]
pub struct ConsumerOptions {
    pub namespace: String,
    pub start_position: Option<FloEventId>,
    pub max_events: u64,
    pub username: String,
    pub password: String,
}

impl ConsumerOptions {
    pub fn simple<S: ToString>(namespace: S, start_position: Option<FloEventId>, max_events: u64) -> ConsumerOptions {
        ConsumerOptions {
            namespace: namespace.to_string(),
            start_position: start_position,
            max_events: max_events,
            username: String::new(),
            password: String::new(),
        }
    }
}

pub trait ConsumerContext {
    fn events_consumed(&self) -> u64;
    fn current_event_id(&self) -> Option<FloEventId>;
    fn respond<N: ToString, D: Into<Vec<u8>>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError>;
}

pub enum ConsumerAction {
    Continue,
    Stop,
}

impl <T, E> From<Result<T, E>> for ConsumerAction {
    fn from(result: Result<T, E>) -> Self {
        if result.is_ok() {
            ConsumerAction::Continue
        } else {
            ConsumerAction::Stop
        }
    }
}


pub trait FloConsumer: Sized {
    fn name(&self) -> &str;
    fn on_event<C: ConsumerContext>(&mut self, event: Result<OwnedFloEvent, &ClientError>, context: &mut C) -> ConsumerAction;
}

pub struct SyncConnection<S: IoStream> {
    stream: ClientStream<S>,
    message_buffer: VecDeque<ProtocolMessage>,
    op_id: u32,
}

impl SyncConnection<TcpStream> {
    pub fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<SyncConnection<TcpStream>> {
        SyncStream::connect(addr).map(|stream| {
            SyncConnection {
                stream: stream,
                message_buffer: VecDeque::new(),
                op_id: 0,
            }
        })
    }

    pub fn from_tcp_stream(tcp_stream: TcpStream) -> SyncConnection<TcpStream> {
        SyncConnection {
            stream: SyncStream::from_stream(tcp_stream),
            op_id: 1,
            message_buffer: VecDeque::new(),
        }
    }
}

impl <S: IoStream> SyncConnection<S> {

    pub fn new(stream: ClientStream<S>) -> SyncConnection<S> {
        SyncConnection {
            stream: stream,
            op_id: 1,
            message_buffer: VecDeque::new(),
        }
    }

    pub fn produce<N: ToString, D: Into<Vec<u8>>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.produce_with_parent(None, namespace, data)
    }

    pub fn produce_with_parent<N: ToString, D: Into<Vec<u8>>>(&mut self, parent_id: Option<FloEventId>, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.op_id += 1;
        let mut send_msg = ProtocolMessage::ProduceEvent(ProduceEvent {
            namespace: namespace.to_string(),
            parent_id: parent_id,
            op_id: self.op_id,
            data: data.into()  //TODO: Make protocolMessage enum generic so the message can just hold a slice
        });

        self.stream.write(&mut send_msg).map_err(|e| e.into()).and_then(|()| {
            self.read_event_ack()
        })
    }

    fn read_next_message(&mut self) -> Result<ProtocolMessage, ClientError> {
        self.message_buffer.pop_front().map(|message| {
            Ok(message)
        }).unwrap_or_else(|| {
            self.stream.read().map_err(|io_err| io_err.into())
        })
    }

    fn read_event_ack(&mut self) -> Result<FloEventId, ClientError> {
        loop {
            match self.stream.read() {
                Ok(ProtocolMessage::AckEvent(ref ack)) if ack.op_id == self.op_id => {
                    return Ok(ack.event_id);
                }
                Ok(ProtocolMessage::Error(error_message)) => {
                    return Err(ClientError::FloError(error_message));
                }
                Ok(other) => {
                    trace!("buffering message: {:?}", other);
                    self.message_buffer.push_back(other);
                }
                Err(io_err) => {
                    return Err(io_err.into());
                }
            }
        }
    }

    pub fn run_consumer<C: FloConsumer>(&mut self, options: ConsumerOptions, consumer: &mut C) -> Result<(), ClientError> {
        let ConsumerOptions{namespace, start_position, username, password, max_events} = options;

        self.authenticate(namespace.clone(), username, password)?;
        if let Some(id) = start_position {
            self.send_event_marker(id)?;
        }
        self.start_consuming(namespace, max_events)?;

        let mut error: Option<ClientError> = None;
        let mut events_consumed = 0;

        while events_consumed < max_events {

            let consumer_action = {
                let read_result = self.read_event();

                if read_result.is_ok() {
                    events_consumed += 1;
                }

                let mut context = ConsumerContextImpl {
                    current_event_id: None,
                    events_consumed: events_consumed,
                    connection: self,
                };

                let for_consumer = match read_result {
                    Ok(event) => {
                        trace!("Client '{}' received event: {:?}", consumer.name(), event.id);
                        context.current_event_id = Some(event.id);
                        Ok(event)
                    }
                    Err(err) => {
                        error!("Consumer: '{}' - Error reading event: {:?}", consumer.name(), err);
                        error = Some(err);
                        Err(error.as_ref().unwrap())
                    }
                };
                consumer.on_event(for_consumer, &mut context)
            };

            match consumer_action {
                ConsumerAction::Continue => {
                    error.take().map(|err| {
                        debug!("Consumer: '{}' - Continuing after error: {:?}", consumer.name(), err);
                    });
                }
                ConsumerAction::Stop => {
                    warn!("Stopping consumer '{}' after error: {:?}", consumer.name(), error);
                    break;
                }
            }

        }

        error.map(|err| {
            Err(err)
        }).unwrap_or(Ok(()))
    }

    fn read_event(&mut self) -> Result<OwnedFloEvent, ClientError> {
        match self.read_next_message() {
            Ok(ProtocolMessage::ReceiveEvent(event)) => Ok(event),
            Ok(ProtocolMessage::Error(err)) => Err(ClientError::FloError(err)),
            Ok(ProtocolMessage::AwaitingEvents) => Err(ClientError::EndOfStream),
            Ok(other) => Err(ClientError::UnexpectedMessage(other)),
            Err(io_err) => Err(io_err.into())
        }
    }

    fn start_consuming(&mut self, namespace: String, max: u64) -> Result<(), ClientError> {
        let mut msg = ProtocolMessage::StartConsuming(ConsumerStart {
            namespace: namespace,
            max_events: max
        });
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

struct ConsumerContextImpl<'a, T: IoStream + 'a> {
    pub events_consumed: u64,
    current_event_id: Option<FloEventId>,
    connection: &'a mut SyncConnection<T>
}

impl <'a, T: IoStream + 'a> ConsumerContext for ConsumerContextImpl<'a, T> {
    fn events_consumed(&self) -> u64 {
        self.events_consumed
    }

    fn current_event_id(&self) -> Option<FloEventId> {
        self.current_event_id
    }

    fn respond<N: ToString, D: Into<Vec<u8>>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.connection.produce_with_parent(self.current_event_id, namespace, data)
    }
}

