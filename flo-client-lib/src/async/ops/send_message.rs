use std::fmt::{self, Debug};
use std::io;

use futures::{Sink, Future, Async, Poll};
use futures::sink::Send;

use protocol::ProtocolMessage;
use async::{AsyncConnection, MessageSender};

pub struct SendMessage<D: Debug> {
    client: Option<AsyncConnection<D>>,
    sender: Send<MessageSender>
}

impl <D: Debug> Debug for SendMessage<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SendMessages{{ client: {:?} }}", self.client)
    }
}

impl <D: Debug> SendMessage<D> {
    pub fn new(mut client: AsyncConnection<D>, message: ProtocolMessage) -> SendMessage<D> {
        let sender = client.send.take().expect("Client.send is missing");

        let send = sender.send(message);

        SendMessage {
            client: Some(client),
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
                client: self.client.take().unwrap(),
                err: io_err,
            }
        }));

        //the SendAll would have already panicked if we tried to poll after completion
        let mut client = self.client.take().unwrap();
        client.send = Some(sender);

        Ok(Async::Ready(client))
    }
}

impl <D: Debug> Into<AsyncConnection<D>> for SendMessage<D> {
    fn into(mut self) -> AsyncConnection<D> {
        self.client.take().expect("SendMessage has already been completed")
    }
}


#[derive(Debug)]
pub struct SendError<D: Debug> {
    pub client: AsyncConnection<D>,
    pub err: io::Error,
}
