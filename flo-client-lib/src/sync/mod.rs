
use std::fmt::Debug;
use std::cell::RefCell;
use std::net::SocketAddr;

use tokio_core::reactor::Core;
use futures::{Future, Stream};

use event::{FloEventId, ActorId, VersionVector};
use async::{AsyncConnection, tcp_connect_with};
use async::ops::{ProduceErr, Consume, ConsumeError};
use codec::EventCodec;
use ::Event;

pub use async::{ErrorType, CurrentStreamState};
pub use async::ops::{EventToProduce, HandshakeError};



thread_local!(static REACTOR: RefCell<Core> = RefCell::new(Core::new().unwrap()));

fn run_future<T, E, F: Future<Item=T, Error=E>>(future: F) -> Result<T, E> {
    REACTOR.with(move |core| {
        core.borrow_mut().run(future)
    })
}


pub struct SyncConnection<D: Debug> {
    async_connection: Option<AsyncConnection<D>>,
}

impl <D: Debug> From<AsyncConnection<D>> for SyncConnection<D> {
    fn from(async_conn: AsyncConnection<D>) -> Self {
        SyncConnection {
            async_connection: Some(async_conn)
        }
    }
}

impl <D: Debug + 'static> SyncConnection<D> {

    pub fn connect_from_str<N, C>(address: &str, client_name: N, codec: C, consume_batch_size: Option<u32>) -> Result<SyncConnection<D>, HandshakeError>
            where N: Into<String>, C: EventCodec<EventData=D> + 'static {
        REACTOR.with(|core| {
            let mut core = core.borrow_mut();
            let handle = core.handle();

            ::std::net::TcpStream::connect(address).and_then(|std_stream| {
                ::tokio_core::net::TcpStream::from_stream(std_stream, &handle)
            }).map_err(|io_err| {
                HandshakeError {
                    message: "Failed to create TCP connection",
                    error_type: io_err.into()
                }
            }).and_then(move |tokio_stream| {
                let boxed_codec = Box::new(codec) as Box<EventCodec<EventData=D>>;
                let conn = AsyncConnection::from_tcp_stream(client_name.into(), tokio_stream, boxed_codec);
                let future = conn.connect_with(consume_batch_size);
                core.run(future)
            }).map(|async_connection| {
                async_connection.into()
            })
        })
    }

    pub fn connect<A, N, C>(address: A, client_name: N, codec: C, consume_batch_size: Option<u32>) -> Result<SyncConnection<D>, HandshakeError>
                    where A: Into<SocketAddr>, N: Into<String>, C: EventCodec<EventData=D> + 'static {

        let result = REACTOR.with(move |core| {
            let mut core = core.borrow_mut();
            let addr = address.into();
            let connect = tcp_connect_with(client_name, &addr, consume_batch_size, codec, &core.handle());
            core.run(connect)
        });
        result.map(|async_conn| async_conn.into())
    }

    pub fn produce(&mut self, event: EventToProduce<D>) -> Result<FloEventId, ErrorType> {
        let conn = self.async_connection.take().unwrap();
        let result = run_future(conn.produce(event));
        match result {
            Ok((id, conn)) => {
                self.async_connection = Some(conn);
                Ok(id)
            }
            Err(ProduceErr {connection, err}) => {
                self.async_connection = Some(connection);
                Err(err)
            }
        }
    }

    pub fn produce_to<N: Into<String>>(&mut self, partition: ActorId, namespace: N, parent_id: Option<FloEventId>, data: D) -> Result<FloEventId, ErrorType> {
        let to_produce = EventToProduce {
            partition,
            namespace: namespace.into(),
            parent_id,
            data
        };
        self.produce(to_produce)
    }

    pub fn into_consumer<N: Into<String>>(mut self, namespace: N, version_vector: &VersionVector, event_limit: Option<u64>, await_new_events: bool) -> EventIterator<D> {
        let connection = self.async_connection.take().unwrap();
        let consume = connection.consume(namespace, version_vector, event_limit, await_new_events);
        EventIterator {
            consume: Some(consume),
            connection: None,
        }
    }

    pub fn current_stream(&self) -> Option<&CurrentStreamState> {
        self.async_connection.as_ref().and_then(|conn| conn.current_stream())
    }

}

/// An iterator of events from an event stream. Each element in the iterator is a `Result<Event<D>, ErrorType>`. If an error is
/// returned, then all future calls to `next()` will return `None`. An `EventIterator` can be converted back into a `SyncConnection`
/// once the consume operation is exhausted.
pub struct EventIterator<D: Debug> {
    /// the async consume `Stream`
    consume: Option<Consume<D>>,

    /// if there's an error, the connection will be stored here until the stream is converted back into the connection
    connection: Option<AsyncConnection<D>>,
}


impl <D: Debug> EventIterator<D> {
    /// Stops the consumer and returns the connection for re-use. Stopping the consumer _may_ require a round trip communication
    /// with the server, so this method returns a `Result` in case there is an error in that process. If an error occurs, the
    /// connection is simply closed since it is possible for it to be left in an invalid state
    pub fn stop_consuming(self) -> Result<SyncConnection<D>, ErrorType> {
        let EventIterator {consume, connection} = self;
        match consume {
            Some(in_progress) => {
                run_future(in_progress.stop()).map(|connection| {
                    connection.into()
                })
            }
            None => {
                // one of either consume or connection must be populated
                Ok(connection.unwrap().into())
            }
        }
    }
}


impl <D: Debug> Iterator for EventIterator<D> {
    type Item = Result<Event<D>, ErrorType>;

    fn next(&mut self) -> Option<Self::Item> {
        self.consume.take().and_then(|consume| {
            REACTOR.with(|core| {
                let result = core.borrow_mut().run(consume.into_future());
                match result {
                    Ok((event, consume_stream)) => {
                        self.consume = Some(consume_stream);
                        event.map(|e| Ok(e))
                    }
                    Err((ConsumeError{connection, error}, _consume_stream)) => {
                        // the consume stream is useless to us in this case
                        self.connection = Some(connection);
                        Some(Err(error))
                    }
                }
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let limit = self.consume.as_ref().and_then(|c| {
            c.get_events_remaining().and_then(|remaining| {
                if remaining <= usize::max_value() as u64 {
                    Some(remaining as usize)
                } else {
                    None
                }
            })
        });
        (0, limit)
    }
}


