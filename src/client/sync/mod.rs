mod client_stream;

use std::io::{self, Write, Read};
use std::time::Duration;
use std::net::{TcpStream, SocketAddr, ToSocketAddrs};

use nom::IResult;

use protocol::{ProtocolMessage, ProduceEventHeader, ReceiveEventHeader, ConsumerStart};
use flo_event::{FloEventId, FloEvent, OwnedFloEvent};
use super::{ClientError, ConsumerOptions};
use std::sync::Mutex;
use std::cell::RefCell;
use std::collections::VecDeque;

pub use self::client_stream::{SyncStream, ClientStream, IoStream};

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

pub trait ConsumerContext {
    fn events_consumed(&self) -> u64;
    fn current_event_id(&self) -> Option<FloEventId>;
    fn respond<N: ToString, D: AsRef<[u8]>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError>;
}

pub struct ConsumerContextImpl<'a, T: IoStream + 'a> {
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

    fn respond<N: ToString, D: AsRef<[u8]>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.connection.produce_with_parent(self.current_event_id, namespace, data)
    }
}

pub trait FloConsumer: Sized {
    fn name(&self) -> &str;
    fn on_event<C: ConsumerContext>(&mut self, event: Result<OwnedFloEvent, &ClientError>, context: &mut C) -> ConsumerAction;
}

enum ClientMessage {
    Event(OwnedFloEvent),
    Proto(ProtocolMessage),
}

pub struct SyncConnection<S: IoStream> {
    stream: ClientStream<S>,
    message_buffer: VecDeque<ClientMessage>,
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

    pub fn produce<N: ToString, D: AsRef<[u8]>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.produce_with_parent(None, namespace, data)
    }

    pub fn produce_with_parent<N: ToString, D: AsRef<[u8]>>(&mut self, parent_id: Option<FloEventId>, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.op_id += 1;
        let mut send_msg = ProtocolMessage::ProduceEvent(ProduceEventHeader {
            namespace: namespace.to_string(),
            parent_id: parent_id,
            op_id: self.op_id,
            data_length: data.as_ref().len() as u32
        });

        self.stream.write(&mut send_msg).map_err(|e| e.into()).and_then(|()| {
            self.stream.write_event_data(data).map_err(|e| e.into())
        }).and_then(|()| {
            self.read_event_ack()
        })
    }

    fn read_next_message(&mut self) -> Result<ClientMessage, ClientError> {
        self.message_buffer.pop_front().map(|message| {
            Ok(message)
        }).unwrap_or_else(|| {
            self.stream.read()
                    .map_err(|io_err| io_err.into())
                    .and_then(|protocol_msg| {
                        match protocol_msg {
                            ProtocolMessage::ReceiveEvent(header) => {
                                self.read_event_data(header).map(|event| {
                                    ClientMessage::Event(event)
                                })
                            }
                            other @ _ => {
                                Ok(ClientMessage::Proto(other))
                            }
                        }
                    })
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
                Ok(ProtocolMessage::ReceiveEvent(header)) => {
                    trace!("buffering event: {:?}", header);
                    let event = self.read_event_data(header)?; //return error if unable to read data
                    self.message_buffer.push_back(ClientMessage::Event(event));
                }
                Ok(other) => {
                    trace!("buffering message: {:?}", other);
                    self.message_buffer.push_back(ClientMessage::Proto(other));
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
        match self.read_next_message() {
            Ok(ClientMessage::Event(event)) => {
                Ok(event)
            },
            Ok(ClientMessage::Proto(ProtocolMessage::Error(error_msg))) => Err(ClientError::FloError(error_msg)),
            Ok(ClientMessage::Proto(other)) => Err(ClientError::UnexpectedMessage(other)),
            Err(io_err) => Err(io_err.into())
        }
    }

    fn read_event_data(&mut self, header: ReceiveEventHeader) -> Result<OwnedFloEvent, ClientError> {
        let data_length = header.data_length;
        self.stream.read_event_data(data_length as usize)
                   .map_err(|io_err| io_err.into())
                   .map(move |event_data| header_into_event(header, event_data))
    }

    fn start_consuming(&mut self, namespace: String, max: u64) -> Result<(), ClientError> {
        let mut msg = ProtocolMessage::StartConsuming(ConsumerStart {
            namespace: namespace,
            max_events: max as i64
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


fn header_into_event(ReceiveEventHeader{id, parent_id, namespace, timestamp, data_length}: ReceiveEventHeader, data: Vec<u8>) -> OwnedFloEvent {
    debug_assert_eq!(data_length as usize, data.len()); //TODO: maybe replace this with explicit error handling

    OwnedFloEvent {
        id: id,
        timestamp: timestamp,
        parent_id: parent_id,
        namespace: namespace,
        data: data,
    }
}
