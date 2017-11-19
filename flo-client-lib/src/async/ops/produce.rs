use std::fmt::Debug;
use std::io;

use futures::{Future, Poll, Async};

use event::{FloEventId, ActorId};
use protocol::{ProtocolMessage, ProduceEvent};
use async::{AsyncConnection, ErrorType, ClientProtocolMessage};
use async::ops::{RequestResponse, RequestResponseError};

/// An operation that produces a single event on an event stream and waits for it to be acknowledged. This future will
/// not resolve until acknowledgement is received from the server that either the event has been persisted successfully or
/// an error occured.
/// If successful, this future resolves to the `FloEventId` of the event produced, along with the connection itself for reuse.
#[derive(Debug)]
#[must_use = "futures must be polled in order to do any work"]
pub struct ProduceOne<D: Debug> {
    #[allow(dead_code)]
    op_id: u32,
    inner: Inner<D>,
}

#[derive(Debug)]
enum Inner<D: Debug> {
    CodecErr(Option<ProduceErr<D>>),
    RequestResp(RequestResponse<D>)
}


impl <D: Debug> ProduceOne<D> {
    pub fn new(mut connection: AsyncConnection<D>, partition: ActorId, namespace: String, parent_id: Option<FloEventId>, data: D) -> ProduceOne<D> {
        let op_id = connection.next_op_id();
        let inner: Inner<D> = match connection.inner.codec.convert_produced(&namespace, data) {
            Ok(converted) => {
                let proto_msg = ProduceEvent{
                    op_id,
                    partition,
                    namespace,
                    parent_id,
                    data: converted,
                };
                Inner::RequestResp(RequestResponse::new(connection, ProtocolMessage::ProduceEvent(proto_msg)))
            }
            Err(codec_err) => {
                let err = ProduceErr {
                    connection: connection,
                    err: ErrorType::Codec(codec_err),
                };
                Inner::CodecErr(Some(err))
            }
        };

        ProduceOne{
            op_id: op_id,
            inner: inner,
        }
    }


    fn response_received(connection: AsyncConnection<D>, response: ClientProtocolMessage) -> Result<Async<(FloEventId, AsyncConnection<D>)>, ProduceErr<D>> {
        match response {
            ProtocolMessage::AckEvent(ack) => {
                Ok(Async::Ready((ack.event_id, connection)))
            }
            ProtocolMessage::Error(err_response) => {
                Err(ProduceErr{
                    connection: connection,
                    err: ErrorType::Server(err_response)
                })
            }
            other @ _ => {
                let io_err = io::Error::new(io::ErrorKind::InvalidData, format!("Invalid response from server: {:?}", other));
                Err(ProduceErr{
                    connection: connection,
                    err: ErrorType::Io(io_err)
                })
            }
        }
    }
}

impl <D: Debug> Future for ProduceOne<D> {
    type Item = (FloEventId, AsyncConnection<D>);
    type Error = ProduceErr<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner {
            Inner::CodecErr(ref mut err) => {
                let produce_err = err.take().expect("Attempted to poll ProduceOne after error completion");
                Err(produce_err)
            }
            Inner::RequestResp(ref mut request) => {
                let (response, connection) = try_ready!(request.poll());
                ProduceOne::response_received(connection, response)
            }
        }
    }
}

impl <D: Debug> Into<AsyncConnection<D>> for ProduceOne<D> {
    fn into(self) -> AsyncConnection<D> {
        match self.inner {
            Inner::RequestResp(rr) => rr.into(),
            Inner::CodecErr(mut err) => {
                err.take().expect("ProduceOne already completed").connection
            }
        }
    }
}

/// Error type when there is a failure to produce an event. Includes the connection itself, in case it can be reused
#[derive(Debug)]
pub struct ProduceErr<D: Debug> {
    pub connection: AsyncConnection<D>,
    pub err: ErrorType,
}


impl <D: Debug> From<RequestResponseError<D>> for ProduceErr<D> {
    fn from(send_err: RequestResponseError<D>) -> Self {
        let RequestResponseError {connection, error} = send_err;
        ProduceErr {
            connection: connection,
            err: ErrorType::Io(error),
        }
    }
}

/// Contains all the required data to produce an event. This is the primary struct used by clients to produce events on a
/// stream.
#[derive(Debug, PartialEq)]
pub struct EventToProduce<D: Debug> {
    pub partition: ActorId,
    pub namespace: String,
    pub parent_id: Option<FloEventId>,
    pub data: D,
}

impl <D: Debug> EventToProduce<D> {
    pub fn new<N: Into<String>>(partition: ActorId, namespace: N, parent_id: Option<FloEventId>, data: D) -> EventToProduce<D> {
        EventToProduce {
            partition,
            namespace: namespace.into(),
            parent_id,
            data
        }
    }

    pub fn witout_parent<N: Into<String>>(partition: ActorId, namespace: N, data: D) -> EventToProduce<D> {
        EventToProduce::new(partition, namespace, None, data)
    }
}


/// An operation that will produce each event from the iterator in sequence. This will wait for acknowledgement of each event
/// before proceeding to the next, and will short circuit the rest of the iterator when an error is encountered.
#[derive(Debug)]
#[must_use = "futures must be polled in order to do any work"]
pub struct ProduceAll<D: Debug, I: Iterator<Item=EventToProduce<D>>> {
    iter: Option<I>,
    current_op: Option<ProduceOne<D>>,
    conn: Option<AsyncConnection<D>>,
    produced_ids: Vec<FloEventId>,
}

impl <D: Debug, I: Iterator<Item=EventToProduce<D>>> ProduceAll<D, I> {
    /// Creates a new `ProduceAll` for the given client which will produce the events in the iterator. An empty iterator
    /// is acceptable, and will just immediately return a successful result when the future is polled
    pub fn new(connection: AsyncConnection<D>, mut iterator: I) -> ProduceAll<D, I> {
        let first_event = iterator.next();

        if let Some(EventToProduce{partition, namespace, parent_id, data}) = first_event {
            let first_op = connection.produce_to(partition, namespace, parent_id, data);
            ProduceAll {
                iter: Some(iterator),
                current_op: Some(first_op),
                conn: None,
                produced_ids: Vec::new(),
            }
        } else {
            // empty produceAll, which will simply yield a result the first time it is polled
            ProduceAll {
                iter: None,
                current_op: None,
                conn: Some(connection),
                produced_ids: Vec::new(),
            }
        }
    }
}

/// The Error returned when any produce operation fails for a `ProduceAll` operation
#[derive(Debug)]
pub struct ProduceAllError<D: Debug> {
    /// The error that was encountered
    pub error: ErrorType,
    /// Information about the successful portion of events produced
    pub result: ProduceAllResult<D>,
}

/// Information about the successful portion of a `ProduceAll` operation. If some of the events are produced successfully,
/// then `events_produced` will be greater than 0 and `last_produced_id` will be set.
#[derive(Debug)]
pub struct ProduceAllResult<D: Debug> {
    pub connection: AsyncConnection<D>,
    pub events_produced: Vec<FloEventId>,
}

impl <D: Debug, I: Iterator<Item=EventToProduce<D>>> Future for ProduceAll<D, I> {
    type Item = ProduceAllResult<D>;
    type Error = ProduceAllError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        {
            let ProduceAll{ref mut iter, ref mut current_op, ref mut conn, ref mut produced_ids} = *self;

            if current_op.is_some() {

                let (id, connection) = {
                    let produce = current_op.as_mut().unwrap();
                    let produce_result = produce.poll().map_err(|err| {
                        let ProduceErr {connection, err} = err;
                        let events_produced = ::std::mem::replace(produced_ids, Vec::new());
                        let res = ProduceAllResult {
                            connection,
                            events_produced
                        };
                        ProduceAllError {
                            error: err,
                            result: res,
                        }
                    });
                    try_ready!(produce_result)
                };
                produced_ids.push(id);
                debug!("Finished producing event: {} with id: {}", produced_ids.len(), id);

                if let Some(next) = iter.as_mut().expect("attempted to poll ProduceAll after it completed with error").next() {
                    let EventToProduce {partition, namespace, parent_id, data} = next;
                    let produce = connection.produce_to(partition, namespace, parent_id, data);
                    *current_op = Some(produce)
                } else {
                    *conn = Some(connection);
                }
            }
        }


        if let Some(connection) = self.conn.take() {
            debug!("Successfully produced all {} events", self.produced_ids.len());
            Ok(Async::Ready(ProduceAllResult {
                connection,
                events_produced: ::std::mem::replace(&mut self.produced_ids, Vec::new()),
            }))
        } else {
            self.poll()
        }

    }
}




