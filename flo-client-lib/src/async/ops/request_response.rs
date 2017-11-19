
use std::io;
use std::fmt::Debug;

use futures::{Future, Async, Poll};

use async::{AsyncConnection, ClientProtocolMessage};
use async::ops::{SendMessage, SendError, AwaitResponse, AwaitResponseError};

#[derive(Debug)]
pub struct RequestResponse<D: Debug> {
    op_id: u32,
    state: State<D>,
}

impl <D: Debug> RequestResponse<D> {
    pub fn new(connection: AsyncConnection<D>, request: ClientProtocolMessage) -> RequestResponse<D> {
        let op_id = request.get_op_id();
        debug_assert_ne!(op_id, 0);
        RequestResponse {
            op_id: op_id,
            state: State::Request(SendMessage::new(connection, request))
        }
    }
}

impl <D: Debug> Future for RequestResponse<D> {
    type Item = (ClientProtocolMessage, AsyncConnection<D>);
    type Error = RequestResponseError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {

        let new_state = match self.state {
            State::Request(ref mut req) => {
                let connection = try_ready!(req.poll()); // early return if sending is not ready
                let await_response = AwaitResponse::new(connection, self.op_id);
                State::Response(await_response)
            }
            State::Response(ref mut resp) => {
                let (resp, connection) = try_ready!(resp.poll()); // early return if reading response is not ready
                return Ok(Async::Ready((resp, connection)));     // early return if reading response _is_ ready :)
            }
        };

        // we only make it here if we were previously in the Request state and it has just completed successfully
        self.state = new_state;
        self.poll()  // make sure to poll the response future to make sure we get polled when it's actually ready
    }
}

impl <D: Debug> Into<AsyncConnection<D>> for RequestResponse<D> {
    fn into(self) -> AsyncConnection<D> {
        match self.state {
            State::Request(send) => send.into(),
            State::Response(response) => response.into()
        }
    }
}


#[derive(Debug)]
pub struct RequestResponseError<D: Debug> {
    pub connection: AsyncConnection<D>,
    pub error: io::Error,
}


impl <D: Debug> From<AwaitResponseError<D>> for RequestResponseError<D> {
    fn from(AwaitResponseError{connection, err}: AwaitResponseError<D>) -> Self {
        RequestResponseError {
            connection: connection,
            error: err
        }
    }
}

impl <D: Debug> From<SendError<D>> for RequestResponseError<D> {
    fn from(SendError{connection, err}: SendError<D>) -> Self {
        RequestResponseError {
            connection: connection,
            error: err
        }
    }
}


#[derive(Debug)]
enum State<D: Debug> {
    Request(SendMessage<D>),
    Response(AwaitResponse<D>)
}

