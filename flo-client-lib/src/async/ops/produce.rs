use std::fmt::{self, Debug};
use std::error::Error;
use std::io;

use futures::{Future, Poll, Async, AsyncSink};

use event::FloEventId;
use codec::EventCodec;
use protocol::{ProtocolMessage, ProduceEvent};
use async::{AsyncClient, ErrorType, MessageSender, MessageReceiver};
use async::ops::{SendMessages, SendError, AwaitResponse, AwaitResponseError};

#[derive(Debug)]
pub struct ProduceOne<D: Debug> {
    op_id: u32,
    inner: Option<Inner<D>>,
}

#[derive(Debug)]
enum Inner<D: Debug> {
    CodecErr(Option<ProduceErr<D>>),
    Send(SendMessages<D>),
    Recv(AwaitResponse<D>),
}


impl <D: Debug> ProduceOne<D> {
    pub fn new(client: AsyncClient<D>, namespace: String, parent_id: Option<FloEventId>, data: D) -> ProduceOne<D> {
        let op_id = client.current_op_id;
        let inner: Inner<D> = match client.codec.convert_produced(&namespace, data) {
            Ok(converted) => {
                let proto_msg = ProduceEvent{
                    op_id: op_id,
                    namespace: namespace,
                    parent_id: parent_id,
                    data: converted,
                };
                Inner::Send(client.send_messages(vec![ProtocolMessage::ProduceEvent(proto_msg)]))
            }
            Err(codec_err) => {
                let err = ProduceErr {
                    client: client,
                    err: ErrorType::Codec(codec_err),
                };
                Inner::CodecErr(Some(err))
            }
        };

        ProduceOne{
            op_id: op_id,
            inner: Some(inner),
        }
    }


    fn response_received(client: AsyncClient<D>, response: ProtocolMessage) -> Result<Async<(FloEventId, AsyncClient<D>)>, ProduceErr<D>> {
        match response {
            ProtocolMessage::AckEvent(ack) => {
                Ok(Async::Ready((ack.event_id, client)))
            }
            ProtocolMessage::Error(err_response) => {
                Err(ProduceErr{
                    client: client,
                    err: ErrorType::Server(err_response)
                })
            }
            other @ _ => {
                let io_err = io::Error::new(io::ErrorKind::InvalidData, format!("Invalid response from server: {:?}", other));
                Err(ProduceErr{
                    client: client,
                    err: ErrorType::Io(io_err)
                })
            }
        }
    }
}

impl <D: Debug> Future for ProduceOne<D> {
    type Item = (FloEventId, AsyncClient<D>);
    type Error = ProduceErr<D>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut new_inner: Option<Inner<D>> = None;
        match self.inner.as_mut() {
            Some(&mut Inner::CodecErr(ref mut err)) => {
                let produce_err = err.take().expect("Attempted to poll ProduceOne after error completion");
                return Err(produce_err);
            }
            Some(&mut Inner::Send(ref mut send)) => {
                let client = try_ready!(send.poll());
                new_inner = Some(Inner::Recv(client.await_response(self.op_id)));
            }
            Some(&mut Inner::Recv(ref mut receiver)) => {
                let (message, client) = try_ready!(receiver.poll());
                return ProduceOne::response_received(client, message)
            }
            None => {
                panic!("Attempted to poll ProduceOne after completion");
            }
        }

        if let Some(inner) = new_inner {
            self.inner = Some(inner);
        }

        return self.poll()
    }
}


#[derive(Debug)]
pub struct ProduceErr<D: Debug> {
    pub client: AsyncClient<D>,
    pub err: ErrorType,
}


impl <D: Debug> From<AwaitResponseError<D>> for ProduceErr<D> {
    fn from(send_err: AwaitResponseError<D>) -> Self {
        let AwaitResponseError {client, err} = send_err;
        ProduceErr {
            client: client,
            err: ErrorType::Io(err),
        }
    }
}

impl <D: Debug> From<SendError<D>> for ProduceErr<D> {
    fn from(send_err: SendError<D>) -> Self {
        let SendError {client, err} = send_err;
        ProduceErr {
            client: client,
            err: ErrorType::Io(err),
        }
    }
}

