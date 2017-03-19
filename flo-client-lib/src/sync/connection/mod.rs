mod transport;
mod options;

use std::net::ToSocketAddrs;
use std::collections::VecDeque;
use std::io;

use codec::EventCodec;
use event::{FloEventId, OwnedFloEvent};
use protocol::{ProtocolMessage, ErrorMessage, ProduceEvent, ConsumerStart};
use sync::{
    ClientError,
    Consumer,
    Context,
    Transport,
    ConsumerAction,
};
use ::Event;

pub use self::options::ConsumerOptions;
pub use self::transport::SyncStream;

/// Convenience type to export for basic consumers to simplify generic type signatures
pub type Connection<T> = SyncConnection<SyncStream, T>;

pub struct SyncConnection<T: Transport + 'static, C: EventCodec + 'static> {
    transport: T,
    codec: C,
    message_buffer: VecDeque<ProtocolMessage>,
    op_id: u32,
}

impl <C: EventCodec> SyncConnection<SyncStream, C> {
    pub fn connect<T: ToSocketAddrs>(addr: T, codec: C) -> io::Result<SyncConnection<SyncStream, C>> {
        SyncStream::connect(addr).map(|stream| {
            SyncConnection::new(stream, codec)
        })
    }
}

impl <T: Transport, C: EventCodec> SyncConnection<T, C> {

    pub fn new(transport: T, codec: C) -> SyncConnection<T, C> {
        SyncConnection {
            transport: transport,
            codec: codec,
            op_id: 1,
            message_buffer: VecDeque::new(),
        }
    }

    pub fn produce<N: ToString, D: Into<C::Body>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.produce_with_parent(None, namespace, data)
    }

    pub fn produce_with_parent<N: ToString, D: Into<C::Body>>(&mut self, parent_id: Option<FloEventId>, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.op_id += 1;

        let namespace_string = namespace.to_string();

        self.codec.convert_produced(&namespace_string, data.into()).map_err(|codec_error| {
            ClientError::Codec(Box::new(codec_error))
        }).and_then(move |binary_data| {
            let mut send_msg = ProtocolMessage::ProduceEvent(ProduceEvent {
                namespace: namespace_string,
                parent_id: parent_id,
                op_id: self.op_id,
                data: binary_data  //TODO: Make protocolMessage enum generic so the message can just hold a slice
            });

            self.send_message(send_msg).and_then(|()| {
                self.read_event_ack()
            })
        })
    }

    fn send_message(&mut self, message: ProtocolMessage) -> Result<(), ClientError> {
        self.transport.send(message).map_err(|err| ClientError::Transport(err))
    }

    fn read_next_message(&mut self) -> Result<ProtocolMessage, ClientError> {
        self.message_buffer.pop_front().map(|message| {
            Ok(message)
        }).unwrap_or_else(|| {
            self.transport.receive().map_err(|io_err| ClientError::Transport(io_err))
        })
    }

    fn read_event_ack(&mut self) -> Result<FloEventId, ClientError> {
        loop {
            match self.transport.receive() {
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
                    return Err(ClientError::Transport(io_err));
                }
            }
        }
    }

    pub fn run_consumer<Con>(&mut self, options: ConsumerOptions, consumer: &mut Con) -> Result<(), ClientError>
        where Con: Consumer<C::Body> {
        let ConsumerOptions{namespace, version_vector, max_events} = options;

        for id in version_vector.snapshot() {
            self.send_event_marker(id)?;
        }
        self.start_consuming(namespace, max_events)?;

        let mut error: Option<ClientError> = None;
        let mut events_consumed = 0;
        let mut event_id = FloEventId::zero();

        while events_consumed < max_events {

            let consumer_action = {
                let read_result = self.read_event();

                if let &Ok(ref event) = &read_result {
                    events_consumed += 1;
                    event_id = event.id;
                }

                match read_result {
                    Ok(event) => {
                        let mut context = ConsumerContextImpl {
                            current_event_id: event_id,
                            events_consumed: events_consumed,
                            connection: self,
                        };
                        trace!("Client '{}' received event: {:?}", consumer.name(), event_id);
                        context.current_event_id = event_id;
                        consumer.on_event(event, &mut context)
                    }
                    Err(err) => {
                        error!("Consumer: '{}' - Error reading event: {:?}", consumer.name(), err);
                        let action = consumer.on_error(&err);
                        error = Some(err);
                        action
                    }
                }
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

    fn read_event(&mut self) -> Result<Event<C::Body>, ClientError> {
        match self.read_next_message() {
            Ok(ProtocolMessage::ReceiveEvent(event)) => self.convert_event(event),
            Ok(ProtocolMessage::Error(err)) => Err(ClientError::FloError(err)),
            Ok(ProtocolMessage::AwaitingEvents) => Err(ClientError::EndOfStream),
            Ok(other) => Err(ClientError::UnexpectedMessage(other)),
            Err(transport_err) => Err(transport_err.into())
        }
    }

    fn convert_event(&self, event: OwnedFloEvent) -> Result<Event<C::Body>, ClientError> {
        let OwnedFloEvent{id, parent_id, namespace, timestamp, data} = event;
        self.codec.convert_received(&namespace, data).map(|converted| {
            Event{
                id: id,
                parent_id: parent_id,
                timestamp: timestamp,
                namespace: namespace,
                data: converted,
            }
        }).map_err(|codec_err| {
            ClientError::Codec(Box::new(codec_err))
        })
    }

    fn start_consuming(&mut self, namespace: String, max: u64) -> Result<(), ClientError> {
        let mut msg = ProtocolMessage::StartConsuming(ConsumerStart {
            namespace: namespace,
            max_events: max
        });
        self.send_message(msg)
    }

    fn send_event_marker(&mut self, id: FloEventId) -> Result<(), ClientError> {
        let mut msg = ProtocolMessage::UpdateMarker(id);
        self.send_message(msg)
    }

}

struct ConsumerContextImpl<'a, T: Transport + 'static, C: EventCodec + 'static> {
    pub events_consumed: u64,
    current_event_id: FloEventId,
    connection: &'a mut SyncConnection<T, C>
}

impl <'a, T: Transport, C: EventCodec> Context<C::Body> for ConsumerContextImpl<'a, T, C> {
    fn current_event_id(&self) -> FloEventId {
        self.current_event_id
    }

    fn respond<N: ToString, D: Into<C::Body>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.connection.produce_with_parent(Some(self.current_event_id), namespace, data)
    }
}
