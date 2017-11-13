
use std::fmt::Debug;
use std::cell::RefCell;
use std::net::SocketAddr;

use tokio_core::reactor::Core;
use futures::{Future, Stream};

use event::{FloEventId, ActorId, VersionVector};
use async::{AsyncConnection, tcp_connect_with};
use async::ops::{ProduceErr, HandshakeError, Consume, ConsumeError};
use codec::EventCodec;
use ::Event;

pub use async::ErrorType;
pub use async::ops::EventToProduce;



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


impl <D: Debug> Into<SyncConnection<D>> for EventIterator<D> {
    // TODO: introduce some safety guarantees about when an iterator is converted back into a connection
    fn into(self) -> SyncConnection<D> {
        let EventIterator {consume, connection} = self;
        // TODO: tell the server that the consumer is stopping
        let connection = consume.map(|stream| stream.into()).or(connection).unwrap();
        SyncConnection {
            async_connection: Some(connection)
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


