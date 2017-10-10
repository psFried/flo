
use std::io;
use std::fmt::Debug;

use futures::{Future, Async, Poll};

use protocol::{ProtocolMessage, ErrorMessage};
use async::{AsyncClient};
use async::ops::{SendMessages, SendError, AwaitResponse, AwaitResponseError};


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
            state: State::Request(SendMessages::new(client, vec![request]))
        }
    }
}

impl <D: Debug> Future for RequestResponse<D> {
    type Item = (ProtocolMessage, AsyncClient<D>);
    type Error = RequestResponseError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut new_state: Option<State<D>> = None;

        match self.state {
            State::Request(ref mut req) => {
                let client = try_ready!(req.poll());
                new_state = Some(State::Response(client.await_response(self.op_id)));
            }
            State::Response(ref mut resp) => {
                let (resp, client) = try_ready!(resp.poll());
                return Ok(Async::Ready((resp, client)));
            }
        }

        if let Some(state) = new_state {
            self.state = state;
            self.poll()
        } else {
            Ok(Async::NotReady)
        }
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
    Request(SendMessages<D>),
    Response(AwaitResponse<D>)
}

