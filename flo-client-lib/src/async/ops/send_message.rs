use std::fmt::{self, Debug};
use std::io;

use futures::{Sink, Future, Async, Poll};
use futures::sink::Send;

use protocol::ProtocolMessage;
use async::{AsyncConnection, MessageSender};

pub struct SendMessage<D: Debug> {
    connection: Option<AsyncConnection<D>>,
    sender: Send<MessageSender>
}

impl <D: Debug> Debug for SendMessage<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SendMessages{{ connection: {:?} }}", self.connection)
    }
}

impl <D: Debug> SendMessage<D> {
    pub fn new(mut connection: AsyncConnection<D>, message: ProtocolMessage) -> SendMessage<D> {
        let sender = connection.take_sender();

        let send = sender.send(message);

        SendMessage {
            connection: Some(connection),
            sender: send,
        }
    }
}


impl <D: Debug> Future for SendMessage<D> {
    type Item = AsyncConnection<D>;
    type Error = SendError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let sender = try_ready!(self.sender.poll().map_err(|io_err| {
            SendError{
                connection: self.connection.take().unwrap(),
                err: io_err,
            }
        }));

        //the SendAll would have already panicked if we tried to poll after completion
        let mut connection = self.connection.take().unwrap();
        connection.return_sender(sender);

        Ok(Async::Ready(connection))
    }
}

impl <D: Debug> Into<AsyncConnection<D>> for SendMessage<D> {
    fn into(mut self) -> AsyncConnection<D> {
        self.connection.take().expect("SendMessage has already been completed")
    }
}


#[derive(Debug)]
pub struct SendError<D: Debug> {
    pub connection: AsyncConnection<D>,
    pub err: io::Error,
}
