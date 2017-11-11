use std::fmt::Debug;
use std::io;

use futures::{Future, Poll, Async};

use event::{FloEventId, ActorId};
use protocol::{ProtocolMessage, ProduceEvent};
use async::{AsyncConnection, ErrorType};
use async::ops::{RequestResponse, RequestResponseError};

#[derive(Debug)]
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


    fn response_received(connection: AsyncConnection<D>, response: ProtocolMessage) -> Result<Async<(FloEventId, AsyncConnection<D>)>, ProduceErr<D>> {
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

