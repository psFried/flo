use std::fmt::{self, Debug};
use std::error::Error;
use std::io;

use futures::{Sink, Stream, AsyncSink, Future, Async, Poll};
use futures::sink::SendAll;

use protocol::ProtocolMessage;
use async::{AsyncClient, MessageSender};

pub struct SendMessages<D: Debug> {
    client: Option<AsyncClient<D>>,
    sender: SendAll<MessageSender, Messages>
}

impl <D: Debug> Debug for SendMessages<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SendMessages{{ client: {:?} }}", self.client)
    }
}

impl <D: Debug> SendMessages<D> {
    pub fn new(mut client: AsyncClient<D>, messages: Vec<ProtocolMessage>) -> SendMessages<D> {
        let sender = client.send.take().expect("Client.send is missing");

        let send_all = sender.send_all(Messages(messages));

        SendMessages {
            client: Some(client),
            sender: send_all,
        }
    }
}


impl <D: Debug> Future for SendMessages<D> {
    type Item = AsyncClient<D>;
    type Error = SendError<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (sender, messages) = try_ready!(self.sender.poll().map_err(|io_err| {
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


pub struct Messages(Vec<ProtocolMessage>);

impl Stream for Messages {
    type Item = ProtocolMessage;
    type Error = ::std::io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(self.0.pop()))
    }
}

#[derive(Debug)]
pub struct SendError<D: Debug> {
    pub client: AsyncClient<D>,
    pub err: io::Error,
}
