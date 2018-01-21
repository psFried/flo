use std::fmt::Debug;
use futures::{Future, Async, Poll};

use protocol::{self, ProtocolMessage};
use async::{AsyncConnection, ErrorType, ClientProtocolMessage};
use async::ops::{RequestResponse, RequestResponseError};

pub struct SetEventStream<D: Debug> {
    inner: RequestResponse<D>,
}

impl <D: Debug> SetEventStream<D> {
    pub fn new(mut connection: AsyncConnection<D>, stream_name: String) -> SetEventStream<D> {
        let op_id = connection.next_op_id();
        let to_send = ProtocolMessage::SetEventStream(protocol::SetEventStream {
            op_id,
            name: stream_name,
        });
        SetEventStream {
            inner: RequestResponse::new(connection, to_send)
        }
    }
}

#[derive(Debug)]
pub struct SetEventStreamError<D: Debug> {
    pub error: ErrorType,
    pub connection: AsyncConnection<D>,
}

impl <D: Debug> From<RequestResponseError<D>> for SetEventStreamError<D> {
    fn from(rr_err: RequestResponseError<D>) -> Self {
        let RequestResponseError {connection, error} = rr_err;
        SetEventStreamError {
            connection,
            error: error.into()
        }
    }
}

impl <D: Debug> Future for SetEventStream<D> {
    type Item = AsyncConnection<D>;
    type Error = SetEventStreamError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (message, mut connection) = try_ready!(self.inner.poll());
        match message {
            ProtocolMessage::StreamStatus(status) => {
                let our_status = status.into();
                connection.inner.current_stream = Some(our_status);
                Ok(Async::Ready(connection))
            }
            ProtocolMessage::Error(err_message) => {
                let err = SetEventStreamError {
                    connection,
                    error: err_message.into(),
                };
                Err(err)
            }
            other @ _ => {
                let err = SetEventStreamError {
                    connection,
                    error: ErrorType::unexpected_message("EventStreamStatus", other)
                };
                Err(err)
            }
        }
    }
}
