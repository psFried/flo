mod transport;
mod options;

use std::net::{ToSocketAddrs, TcpStream};
use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;

use codec::EventCodec;
use event::{FloEventId, OwnedFloEvent, VersionVector};
use protocol::{ProtocolMessage, ProduceEvent, ConsumerStart};
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
pub type Connection<B, C> = SyncConnection<SyncStream, B, C>;

pub struct SyncConnection<T: Transport + 'static, B: 'static, C: EventCodec<B> + 'static> {
    transport: T,
    codec: C,
    message_buffer: VecDeque<ProtocolMessage>,
    batch_size: u32,
    batch_remaining: u32,
    op_id: u32,
    _phantom_data: PhantomData<B>,
}

impl <B: 'static, C: EventCodec<B>> SyncConnection<SyncStream, B, C> {
    pub fn connect<T: ToSocketAddrs>(addr: T, codec: C) -> io::Result<SyncConnection<SyncStream, B, C>> {
        SyncStream::connect(addr).map(|stream| {
            SyncConnection::new(stream, codec)
        })
    }

    pub fn from_tcp_stream(stream: TcpStream, codec: C) -> SyncConnection<SyncStream, B, C>  {
        SyncConnection::new(SyncStream::from_stream(stream), codec)
    }
}

impl <T: Transport, B, C: EventCodec<B>> SyncConnection<T, B, C> {

    pub fn new(transport: T, codec: C) -> SyncConnection<T, B, C> {
        SyncConnection {
            transport: transport,
            codec: codec,
            batch_size: 0,
            batch_remaining: 0,
            op_id: 1,
            message_buffer: VecDeque::new(),
            _phantom_data: PhantomData,
        }
    }

    /// Sets the batch size for the server to use when sending events to the consumer
    pub fn set_batch_size(&mut self, batch_size: u32) -> Result<(), ClientError> {
        //TODO: this is currently a fire-and-forget method. It returns as soon as the protocol message is written to the transport layer and does not wait for an acknowledgement from the server. We may or may not want to have the server explicitly ack these messages.
        self.transport.send(ProtocolMessage::SetBatchSize(batch_size)).map_err(|e| e.into())
    }

    pub fn produce<N: ToString, D: Into<B>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.produce_with_parent(None, namespace, data)
    }

    pub fn produce_with_parent<N: ToString, D: Into<B>>(&mut self, parent_id: Option<FloEventId>, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.op_id += 1;

        let namespace_string = namespace.to_string();

        self.codec.convert_produced(&namespace_string, data.into()).map_err(|codec_error| {
            ClientError::Codec(Box::new(codec_error))
        }).and_then(move |binary_data| {
            let send_msg = ProtocolMessage::ProduceEvent(ProduceEvent {
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

    pub fn tail_stream<'a, N: Into<String>>(&'a mut self, namespace: N, starting_point: VersionVector) -> Result<EventIter<'a, T, B, C>, ClientError> {
        let options = ConsumerOptions::new(namespace, starting_point, ::std::u64::MAX);
        self.iter(options, true)
    }

    pub fn query<'a, N: Into<String>>(&'a mut self, namespace: N, starting_point: VersionVector, max_events: u64) -> Result<EventIter<'a, T, B, C>, ClientError> {
        let options = ConsumerOptions::new(namespace, starting_point, max_events);
        self.iter(options, false)
    }

    pub fn iter<'a>(&'a mut self, options: ConsumerOptions, await_new_events: bool) -> Result<EventIter<'a, T, B, C>, ClientError> {
        let ConsumerOptions{namespace, version_vector, max_events} = options;

        for id in version_vector.snapshot() {
            self.send_event_marker(id)?;
        }
        self.start_consuming(namespace, max_events)?;
        Ok(EventIter {
            connection: self,
            events_consumed: 0,
            await_new_events: await_new_events,
        })
    }

    pub fn run_consumer<Con>(&mut self, options: ConsumerOptions, consumer: &mut Con) -> Result<(), ClientError>
        where Con: Consumer<B> {
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
                            batch_remaining: self.batch_remaining,
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

        // Sends a StopConsuming message to the server to let it know that we're done
        let stop_result = self.stop_consuming();

        match error {
            None => stop_result,
            Some(e) => {
                // ignore the error from stopping, since the earlier error will likely be more informative
                Err(e)
            }
        }
    }

    fn stop_consuming(&mut self) -> Result<(), ClientError> {
        debug!("stop_consuming");
        // Reset batch state just so a debug output of the connection doesn't confuse anyone
        self.batch_size = 0;
        self.batch_remaining = 0;
        self.transport.send(ProtocolMessage::StopConsuming).map_err(|e| e.into())
    }

    fn read_event(&mut self) -> Result<Event<B>, ClientError> {
        match self.read_next_message() {
            Ok(ProtocolMessage::EndOfBatch) => {
                // if we've hit the end of the batch, then we need to tell the server to send more
                self.transport.send(ProtocolMessage::NextBatch).map_err(|e| e.into()).and_then(|()| {
                    // restart batch
                    self.batch_remaining = self.batch_size;
                    self.read_event()
                })
            }
            Ok(ProtocolMessage::ReceiveEvent(event)) => {
                self.batch_remaining -= 1;
                self.convert_event(event)
            },
            Ok(ProtocolMessage::Error(err)) => Err(ClientError::FloError(err)),
            Ok(ProtocolMessage::AwaitingEvents) => Err(ClientError::EndOfStream),
            Ok(other) => {
                error!("Client received unexpected message when trying to read next event: {:?}", other);
                Err(ClientError::UnexpectedMessage(other))
            },
            Err(transport_err) => Err(transport_err.into())
        }
    }

    fn convert_event(&self, event: OwnedFloEvent) -> Result<Event<B>, ClientError> {
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
        let op_id = self.next_op_id();
        let msg = ProtocolMessage::StartConsuming(ConsumerStart {
            op_id: op_id,
            namespace: namespace,
            max_events: max
        });

        self.send_message(msg)?; // early return if sending the start message fails
        let response = self.read_response(op_id)?; // early return if reading response fails

        match response {
            ProtocolMessage::CursorCreated(cursor_info) => {
                // initialize batch state
                self.batch_size = cursor_info.batch_size;
                self.batch_remaining = cursor_info.batch_size;
                Ok(())
            }
            ProtocolMessage::Error(err) => {
                Err(ClientError::FloError(err))
            }
            other @ _ => {
                warn!("Client received unexpected message: {:?}, Expected CursorCreated", other);
                Err(ClientError::UnexpectedMessage(other))
            }
        }
    }

    fn read_response(&mut self, op_id: u32) -> Result<ProtocolMessage, ClientError> {
        loop {
            let next_message = self.read_next_message()?;
            if next_message.get_op_id() == op_id {
                debug!("Read response for op_id: {}", op_id);
                return Ok(next_message);
            } else {
                debug!("Discarding message: {:?} while awaiting response for op_id: {}", next_message, op_id);
            }
        }
    }

    fn send_event_marker(&mut self, id: FloEventId) -> Result<(), ClientError> {
        let msg = ProtocolMessage::UpdateMarker(id);
        self.send_message(msg)
    }

    fn next_op_id(&mut self) -> u32 {
        self.op_id += 1;
        self.op_id
    }

}

struct ConsumerContextImpl<'a, T: Transport + 'static, B: 'static, C: EventCodec<B> + 'static> {
    pub events_consumed: u64,
    batch_remaining: u32,
    current_event_id: FloEventId,
    connection: &'a mut SyncConnection<T, B, C>
}

impl <'a, T: Transport, B: 'static, C: EventCodec<B>> Context<B> for ConsumerContextImpl<'a, T, B, C> {
    fn current_event_id(&self) -> FloEventId {
        self.current_event_id
    }

    fn respond<N: ToString, D: Into<B>>(&mut self, namespace: N, data: D) -> Result<FloEventId, ClientError> {
        self.connection.produce_with_parent(Some(self.current_event_id), namespace, data)
    }

    fn batch_remaining(&self) -> u32 {
        self.batch_remaining
    }
}


pub struct EventIter<'a, T: Transport + 'static, B: 'static, C: EventCodec<B> + 'static> {
    connection: &'a mut SyncConnection<T, B, C>,
    events_consumed: u64,
    await_new_events: bool,
}

impl <'a, T: Transport + 'static, B: 'static, C: EventCodec<B> + 'static> Iterator for EventIter<'a, T, B, C> {
    type Item = Result<Event<B>, ClientError>;

    fn next(&mut self) -> Option<Self::Item> {
        let read_result = self.connection.read_event();

        match read_result {
            Err(ClientError::EndOfStream) if self.await_new_events => {
                self.next()
            }
            Err(ClientError::EndOfStream) => {
                None
            }
            other @ _ => Some(other)
        }

    }
}

impl <'a, T: Transport + 'static, B: 'static, C: EventCodec<B> + 'static> ::std::ops::Drop for EventIter<'a, T, B, C> {
    fn drop(&mut self) {
        let result = self.connection.stop_consuming();
        if let Err(err) = result {
            error!("StopConsuming resulted in error: {:?}", err);
        }
    }
}
