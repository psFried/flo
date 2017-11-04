
use std::io;
use std::fmt::Debug;

use futures::{Future, Async, Poll};

use protocol::{ProtocolMessage};
use async::{AsyncClient};
use async::ops::{SendMessage, SendError, AwaitResponse, AwaitResponseError};


pub struct RequestResponse<D: Debug> {
    op_id: u32,
    state: State<D>,
}

impl <D: Debug> RequestResponse<D> {
    pub fn new(client: AsyncClient<D>, request: ProtocolMessage) -> RequestResponse<D> {
        let op_id = request.get_op_id();
        debug_assert_ne!(op_id, 0);
        RequestResponse {
            op_id: op_id,
            state: State::Request(SendMessage::new(client, request))
        }
    }
}

impl <D: Debug> Future for RequestResponse<D> {
    type Item = (ProtocolMessage, AsyncClient<D>);
    type Error = RequestResponseError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {

        let new_state = match self.state {
            State::Request(ref mut req) => {
                let client = try_ready!(req.poll()); // early return if sending is not ready
                let await_response = AwaitResponse::new(client, self.op_id);
                State::Response(await_response)
            }
            State::Response(ref mut resp) => {
                let (resp, client) = try_ready!(resp.poll()); // early return if reading response is not ready
                return Ok(Async::Ready((resp, client)));     // early return if reading response _is_ ready :)
            }
        };

        // we only make it here if we were previously in the Request state and it has just completed successfully
        self.state = new_state;
        self.poll()  // make sure to poll the response future to make sure we get polled when it's actually ready
    }
}


#[derive(Debug)]
pub struct RequestResponseError<D: Debug> {
    pub client: AsyncClient<D>,
    pub error: io::Error,
}


impl <D: Debug> From<AwaitResponseError<D>> for RequestResponseError<D> {
    fn from(AwaitResponseError{client, err}: AwaitResponseError<D>) -> Self {
        RequestResponseError {
            client: client,
            error: err
        }
    }
}

impl <D: Debug> From<SendError<D>> for RequestResponseError<D> {
    fn from(SendError{client, err}: SendError<D>) -> Self {
        RequestResponseError {
            client: client,
            error: err
        }
    }
}


enum State<D: Debug> {
    Request(SendMessage<D>),
    Response(AwaitResponse<D>)
}

