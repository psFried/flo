
use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::error::Error;
use std::io;

use futures::{Future, Async, Poll};

use protocol::ProtocolMessage;
use async::{AsyncClient, ErrorType};


#[derive(Debug)]
pub struct AwaitResponse<D: Debug> {
    op_id: u32,
    client: Option<AsyncClient<D>>,
    buffered_message: Option<ProtocolMessage>,
}


impl <D: Debug> AwaitResponse<D> {

    pub fn new(mut client: AsyncClient<D>, op_id: u32) -> AwaitResponse<D> {
        // first check to see if we happen to have the response already buffered.
        let buffered: Option<ProtocolMessage> = {
            let buf: &mut VecDeque<ProtocolMessage> = &mut client.received_message_buffer;
            let index = buf.iter().enumerate().find(|&(idx, ref message)| {
                message.get_op_id() == op_id
            }).map(|(idx, _)| idx);
            index.and_then(|idx| {
                trace!("Found buffered response for op_id: {}", op_id);
                buf.remove(idx)
            })
        };

        AwaitResponse {
            op_id: op_id,
            client: Some(client),
            buffered_message: buffered,
        }
    }

    fn can_buffer_received(&self) -> bool {
        self.client.as_ref().map(|client| client.can_buffer_received()).unwrap_or(false)
    }

    fn buffer_received(&mut self, message: ProtocolMessage) {
        let client = self.client.as_mut().unwrap();
        client.buffer_received(message);
    }
}



impl <D: Debug> Future for AwaitResponse<D> {
    type Item = (ProtocolMessage, AsyncClient<D>);
    type Error = AwaitResponseError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(message) = self.buffered_message.take() {
            let client = self.client.take().unwrap();
            return Ok(Async::Ready((message, client)));
        }

        loop {
            let message: ProtocolMessage = match self.client.as_mut().expect("Attempt to poll AwaitResponse after completion").recv.as_mut().unwrap().poll() {
                Ok(Async::Ready(Some(msg))) => msg,
                Ok(Async::Ready(None)) => {
                    let err = io::Error::new(io::ErrorKind::UnexpectedEof, format!("Got EOF before response to op_id: {}", self.op_id));
                    return Err(AwaitResponseError{
                        client: self.client.take().unwrap(),
                        err: err,
                    });
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(io_err) => {
                    trace!("Error receiving next message awaiting repsonse for op_id: {}", self.op_id);
                    return Err(AwaitResponseError{
                        client: self.client.take().unwrap(),
                        err: io_err,
                    });
                }
            };

            if message.get_op_id() == self.op_id {
                return Ok(Async::Ready((message, self.client.take().unwrap())));
            } else if self.can_buffer_received() {
                // loop around for another try
                self.buffer_received(message);
            } else {
                let err_message = format!("Filled receive buffer before getting response for op_id: {}", self.op_id);
                let err = io::Error::new(io::ErrorKind::Other, err_message);
                let client = self.client.take().unwrap();
                return Err(AwaitResponseError{
                    client: client,
                    err: err,
                });
            }
        }
    }
}


pub struct AwaitResponseError<D: Debug> {
    pub client: AsyncClient<D>,
    pub err: io::Error,
}
