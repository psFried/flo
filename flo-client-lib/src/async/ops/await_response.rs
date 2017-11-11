
use std::collections::VecDeque;
use std::fmt::Debug;
use std::io;

use futures::{Future, Async, Poll};

use protocol::ProtocolMessage;
use async::{AsyncConnection};


#[derive(Debug)]
pub struct AwaitResponse<D: Debug> {
    op_id: u32,
    connection: Option<AsyncConnection<D>>,
    buffered_message: Option<ProtocolMessage>,
}


impl <D: Debug> AwaitResponse<D> {

    pub fn new(mut connection: AsyncConnection<D>, op_id: u32) -> AwaitResponse<D> {
        // first check to see if we happen to have the response already buffered.
        let buffered: Option<ProtocolMessage> = {
            let buf: &mut VecDeque<ProtocolMessage> = &mut connection.inner.received_message_buffer;
            let index = buf.iter().enumerate().find(|&(_, ref message)| {
                message.get_op_id() == op_id
            }).map(|(idx, _)| idx);
            index.and_then(|idx| {
                trace!("Found buffered response for op_id: {}", op_id);
                buf.remove(idx)
            })
        };

        AwaitResponse {
            op_id: op_id,
            connection: Some(connection),
            buffered_message: buffered,
        }
    }

    fn can_buffer_received(&self) -> bool {
        self.connection.as_ref().map(|connection| connection.can_buffer_received()).unwrap_or(false)
    }

    fn buffer_received(&mut self, message: ProtocolMessage) {
        let connection = self.connection.as_mut().unwrap();
        connection.buffer_received(message);
    }
}



impl <D: Debug> Future for AwaitResponse<D> {
    type Item = (ProtocolMessage, AsyncConnection<D>);
    type Error = AwaitResponseError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(message) = self.buffered_message.take() {
            let connection = self.connection.take().unwrap();
            return Ok(Async::Ready((message, connection)));
        }

        loop {
            let msg_result = self.connection.as_mut().expect("Attempt to poll AwaitResponse after completion").inner.recv.as_mut().unwrap().poll();
            let message: ProtocolMessage = match msg_result {
                Ok(Async::Ready(Some(msg))) => msg,
                Ok(Async::Ready(None)) => {
                    let err = io::Error::new(io::ErrorKind::UnexpectedEof, format!("Got EOF before response to op_id: {}", self.op_id));
                    return Err(AwaitResponseError{
                        connection: self.connection.take().unwrap(),
                        err: err,
                    });
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(io_err) => {
                    trace!("Error receiving next message awaiting repsonse for op_id: {}", self.op_id);
                    return Err(AwaitResponseError{
                        connection: self.connection.take().unwrap(),
                        err: io_err,
                    });
                }
            };

            if message.get_op_id() == self.op_id {
                return Ok(Async::Ready((message, self.connection.take().unwrap())));
            } else if self.can_buffer_received() {
                // loop around for another try
                trace!("Buffering message because await_response_op_id: {} did not match message: {:?}", self.op_id, message);
                self.buffer_received(message);
            } else {
                let err_message = format!("Filled receive buffer before getting response for op_id: {}", self.op_id);
                let err = io::Error::new(io::ErrorKind::Other, err_message);
                let connection = self.connection.take().unwrap();
                return Err(AwaitResponseError{
                    connection: connection,
                    err: err,
                });
            }
        }
    }
}

impl <D: Debug> Into<AsyncConnection<D>> for AwaitResponse<D> {
    fn into(mut self) -> AsyncConnection<D> {
        self.connection.take().expect("AwaitResponse has already been completed")
    }
}


#[derive(Debug)]
pub struct AwaitResponseError<D: Debug> {
    pub connection: AsyncConnection<D>,
    pub err: io::Error,
}
