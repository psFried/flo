
use std::fmt::{self, Display, Debug};
use std::io;

use futures::{Future, Async, Poll};

use protocol::{ProtocolMessage, ClientAnnounce};
use async::{AsyncConnection, ErrorType};
use async::ops::{RequestResponse, RequestResponseError};

const PROTOCOL_VERSION: u32 = 1;

pub struct Handshake<D: Debug> {
    request_response: RequestResponse<D>
}

impl <D: Debug> Handshake<D> {
    pub fn new(mut connection: AsyncConnection<D>) -> Handshake<D> {
        let op_id = connection.next_op_id();
        let batch_size = connection.inner.recv_batch_size;
        let request = ProtocolMessage::Announce(ClientAnnounce{
            protocol_version: PROTOCOL_VERSION,
            op_id: op_id,
            client_name: connection.inner.client_name.clone(),
            consume_batch_size: batch_size,
        });
        let inner = RequestResponse::new(connection, request);

        Handshake {
            request_response: inner
        }
    }
}

impl <D: Debug> Future for Handshake<D> {
    type Item = AsyncConnection<D>;
    type Error = HandshakeError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (response, connection) = try_ready!(self.request_response.poll());
        result_from_response(response, connection)
    }
}

impl <D: Debug> Into<AsyncConnection<D>> for Handshake<D> {
    fn into(self) -> AsyncConnection<D> {
        self.request_response.into()
    }
}

fn result_from_response<D: Debug>(response: ProtocolMessage, mut connection: AsyncConnection<D>) -> Poll<AsyncConnection<D>, HandshakeError> {
    debug!("Received Response: {:?}", response);

    match response {
        ProtocolMessage::StreamStatus(status) => {
            // this is the response we are expecting
            let our_status = status.into();
            connection.inner.current_stream = Some(our_status);
            Ok(Async::Ready(connection))
        }
        ProtocolMessage::Error(err_msg) => {
            Err(HandshakeError {
                message: "Server error",
                error_type: ErrorType::Server(err_msg),
            })
        }
        other @ _ => {
            // bad, bad, not good
            let err_msg = format!("Received unexpected message, expected: StreamStatus message, got: {:?}", other);
            error!("{}", err_msg);
            Err(HandshakeError {
                message: "Unexpected message from server",
                error_type: ErrorType::Io(io::Error::new(io::ErrorKind::InvalidData, err_msg))
            })
        }
    }
}


#[derive(Debug)]
pub struct HandshakeError {
    pub message: &'static str,
    pub error_type: ErrorType,
}

impl <D: Debug> From<RequestResponseError<D>> for HandshakeError {
    fn from(err: RequestResponseError<D>) -> Self {
        HandshakeError {
            message: "Failed to connect to server",
            error_type: ErrorType::Io(err.error)
        }
    }
}


impl From<io::Error> for HandshakeError {
    fn from(io_err: io::Error) -> Self {
        HandshakeError {
            message: "IO Error during connection",
            error_type: io_err.into(),
        }
    }
}

impl Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error: '{}', caused by: {:?}", self.message, self.error_type)
    }
}
