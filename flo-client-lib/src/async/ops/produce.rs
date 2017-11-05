use std::fmt::Debug;
use std::io;

use futures::{Future, Poll, Async};

use event::FloEventId;
use protocol::{ProtocolMessage, ProduceEvent};
use async::{AsyncClient, ErrorType};
use async::ops::{RequestResponse, RequestResponseError};

#[derive(Debug)]
pub struct ProduceOne<D: Debug> {
    #[allow(dead_code)]
    op_id: u32,
    inner: Inner<D>,
}

#[derive(Debug)]
enum Inner<D: Debug> {
    CodecErr(Option<ProduceErr<D>>),
    RequestResp(RequestResponse<D>)
}


impl <D: Debug> ProduceOne<D> {
    pub fn new(mut client: AsyncClient<D>, namespace: String, parent_id: Option<FloEventId>, data: D) -> ProduceOne<D> {
        let op_id = client.next_op_id();
        let inner: Inner<D> = match client.codec.convert_produced(&namespace, data) {
            Ok(converted) => {
                let proto_msg = ProduceEvent{
                    op_id: op_id,
                    namespace: namespace,
                    parent_id: parent_id,
                    data: converted,
                };
                Inner::RequestResp(RequestResponse::new(client, ProtocolMessage::ProduceEvent(proto_msg)))
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
            inner: inner,
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
        match self.inner {
            Inner::CodecErr(ref mut err) => {
                let produce_err = err.take().expect("Attempted to poll ProduceOne after error completion");
                Err(produce_err)
            }
            Inner::RequestResp(ref mut request) => {
                let (response, client) = try_ready!(request.poll());
                ProduceOne::response_received(client, response)
            }
        }
    }
}


#[derive(Debug)]
pub struct ProduceErr<D: Debug> {
    pub client: AsyncClient<D>,
    pub err: ErrorType,
}


impl <D: Debug> From<RequestResponseError<D>> for ProduceErr<D> {
    fn from(send_err: RequestResponseError<D>) -> Self {
        let RequestResponseError {client, error} = send_err;
        ProduceErr {
            client: client,
            err: ErrorType::Io(error),
        }
    }
}

