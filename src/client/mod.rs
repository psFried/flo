


pub use hyper::client::{Client, IntoUrl, Response};
pub use hyper::Url;
use hyper::status::StatusCode;

use std::io::Read;
use serde_json::Value;
use event::EventId;

pub struct FloClient {
    server_url: Url,
}

impl FloClient {

    pub fn new<U: IntoUrl>(server_url: U) -> Result<FloClient, String> {
        server_url.into_url().map(|parsed_url| {
            FloClient {
                server_url: parsed_url
            }
        }).map_err(|e| {
            format!("Error parsing url - {:?}", e)
        })
    }

    pub fn create_producer(&self) -> Producer {
        let client = Client::new();
        Producer {
            hyper_client: client,
            server_url: self.server_url.clone(),
        }
    }
}

pub type EventJson = Value;

pub type ProducerErr = String;

pub struct Producer {
    hyper_client: Client,
    server_url: Url,
}

impl Producer {

    pub fn produce(&mut self, event: &EventJson) -> Result<EventId, ProducerErr> {
        use serde_json::ser::to_vec;

        to_vec(event).map_err(|json_err| {
            format!("Error serializing event: {:?}", json_err)
        }).and_then(move |json_bytes| {
            self.hyper_client.put(self.server_url.clone())
                    .body(json_bytes.as_slice())
                    .send().map_err(|hyper_err| {
                        format!("Error executing request - {:?}", hyper_err)
            }).and_then(|response| {
                Producer::read_response(response)
            })
        })
    }

    fn read_response(response: Response) -> Result<EventId, ProducerErr> {
        use serde_json::de::from_reader;

        const RESPONSE_EVENT_ID: &'static str = "eventId";

        if let StatusCode::Ok = response.status {
            from_reader(response).map_err(|parse_err| {
                format!("Error parsing response from server - {:?}", parse_err)
            }).and_then(|response_json: Value| {
                response_json.find(RESPONSE_EVENT_ID).and_then(|event_id_value| {
                    event_id_value.as_u64()
                }).ok_or_else(|| {
                    format!("Error parsing eventId received from server - {:?}", response_json)
                })
            })
        } else {
            Err(format!("Response from server: {:?}", response.status))
        }
    }
}
