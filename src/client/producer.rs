use serde_json;
use event::{self, EventId, Json, EventJson};
use hyper::client::{Client, Response};
use hyper::Url;
use std::sync::Arc;
use super::ConsumerCommand;


pub type ProducerResult = Result<EventId, ProducerError>;

#[derive(Debug, Clone, PartialEq)]
pub enum ProducerError {
    InvalidJson,
    ProducerShutdown,
    ServerError(String),
    Wtf(String),
}


pub struct FloProducer {
    client_ref: Arc<Client>,
    server_url: Url,
}

impl FloProducer {
    pub fn new(server_url: Url, hyper_client: Arc<Client>) -> FloProducer {
        FloProducer {
            server_url: server_url,
            client_ref: hyper_client,
        }
    }

    pub fn default(server_url: Url) -> FloProducer {
        FloProducer::new(server_url, Arc::new(Client::new()))
    }

    pub fn emit<T: EventJson>(&self, event_json: T) -> ProducerResult {
        emit(&*self.client_ref, self.server_url.clone(), event_json)
    }

    pub fn emit_raw(&self, bytes: &[u8]) -> ProducerResult {
        emit_raw(&*self.client_ref, self.server_url.clone(), bytes)
    }

    pub fn produce_stream<'a, S, I, F>(&self, stream: S, handler: &mut F)
        where S: Iterator<Item = I>,
              I: EventJson,
              F: StreamProducerHandler<I>
    {

        produce_stream(&*self.client_ref, self.server_url.clone(), stream, handler)
    }
}

pub trait StreamProducerHandler<T: EventJson> {
    fn handle_result(&mut self, result: ProducerResult, json: T) -> ConsumerCommand;
}

impl<F, T> StreamProducerHandler<T> for F
    where F: Fn(ProducerResult, T) -> ConsumerCommand,
          T: EventJson
{
    fn handle_result(&mut self, result: ProducerResult, json: T) -> ConsumerCommand {
        self(result, json)
    }
}

pub fn produce_stream<'a, S, I, F>(client: &Client, url: Url, json_iter: S, handler: &mut F)
    where S: Iterator<Item = I>,
          I: EventJson,
          F: StreamProducerHandler<I>
{

    for json in json_iter {
        let result = if let Ok(bytes) = json.to_bytes() {
            emit_raw(client, url.clone(), &bytes)
        } else {
            Err(ProducerError::InvalidJson)
        };
        if let ConsumerCommand::Stop(_) = handler.handle_result(result, json) {
            break;
        }
    }

}


pub fn emit<T: EventJson>(client: &Client, url: Url, json: T) -> ProducerResult {
    event::to_bytes(json.json())
        .map_err(|_err| ProducerError::InvalidJson)
        .and_then(|bytes| emit_raw(client, url, &bytes))
}

pub fn emit_raw(client: &Client, url: Url, bytes: &[u8]) -> ProducerResult {
    client.put(url)
          .body(bytes)
          .send()
          .map_err(|hyper_err| ProducerError::Wtf(format!("http error: {:?}", hyper_err)))
          .and_then(|mut response| parse_response(&mut response))
}

fn parse_response(response: &mut Response) -> ProducerResult {
    serde_json::from_reader(response)
        .map_err(|err| ProducerError::ServerError(format!("Received bogus response from server: {:?}", err)))
        .and_then(|response_json: Json| {
            response_json.find("id")
                         .and_then(Json::as_u64)
                         .ok_or_else(|| ProducerError::Wtf("event was not saved".to_string()))
        })
}
