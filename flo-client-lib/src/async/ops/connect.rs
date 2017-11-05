
use std::fmt::Debug;
use std::io;

use futures::{Future, Async, Poll};

use protocol::{ProtocolMessage, ClientAnnounce};
use async::{AsyncClient, ErrorType};
use async::ops::{RequestResponse, RequestResponseError};

const PROTOCOL_VERSION: u32 = 1;

pub struct ConnectAsyncClient<D: Debug> {
    request_response: RequestResponse<D>
}

impl <D: Debug> ConnectAsyncClient<D> {
    pub fn new(mut client: AsyncClient<D>) -> ConnectAsyncClient<D> {
        let op_id = client.next_op_id();
        let batch_size = client.recv_batch_size;
        let request = ProtocolMessage::Announce(ClientAnnounce{
            protocol_version: PROTOCOL_VERSION,
            op_id: op_id,
            client_name: client.client_name.clone(),
            consume_batch_size: batch_size,
        });
        let inner = RequestResponse::new(client, request);

        ConnectAsyncClient {
            request_response: inner
        }
    }
}

impl <D: Debug> Future for ConnectAsyncClient<D> {
    type Item = AsyncClient<D>;
    type Error = ConnectClientError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (response, client) = try_ready!(self.request_response.poll());
        result_from_response(response, client)
    }
}

fn result_from_response<D: Debug>(response: ProtocolMessage, mut client: AsyncClient<D>) -> Poll<AsyncClient<D>, ConnectClientError> {
    debug!("Received Response: {:?}", response);

    match response {
        ProtocolMessage::StreamStatus(status) => {
            // this is the response we are expecting
            let our_status = status.into();
            client.current_stream = Some(our_status);
            Ok(Async::Ready(client))
        }
        ProtocolMessage::Error(err_msg) => {
            Err(ConnectClientError {
                message: "Server error",
                error_type: ErrorType::Server(err_msg),
            })
        }
        other @ _ => {
            // bad, bad, not good
            let err_msg = format!("Received unexpected message, expected: StreamStatus message, got: {:?}", other);
            error!("{}", err_msg);
            Err(ConnectClientError {
                message: "Unexpected message from server",
                error_type: ErrorType::Io(io::Error::new(io::ErrorKind::InvalidData, err_msg))
            })
        }
    }
}


#[derive(Debug)]
pub struct ConnectClientError {
    pub message: &'static str,
    pub error_type: ErrorType,
}

impl <D: Debug> From<RequestResponseError<D>> for ConnectClientError {
    fn from(err: RequestResponseError<D>) -> Self {
        ConnectClientError {
            message: "Failed to connect to server",
            error_type: ErrorType::Io(err.error)
        }
    }
}


impl From<io::Error> for ConnectClientError {
    fn from(io_err: io::Error) -> Self {
        ConnectClientError {
            message: "IO Error during connection",
            error_type: io_err.into(),
        }
    }
}
