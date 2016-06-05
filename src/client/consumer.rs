use event::Event;

use hyper::client::{Client, Response};
use hyper::Url;

use std::io::Read;
use std::time::Duration;

use super::{ConsumerCommand, StopResult};


pub trait FloConsumer {
    fn on_event(&mut self, _event: Event) -> ConsumerCommand {
        ConsumerCommand::Stop(Err("oh shit man!".to_string()))
    }
}

pub fn run_consumer<T: FloConsumer>(consumer: &mut T, url: Url, timeout: Duration) -> StopResult {
    let mut client = Client::new();
    client.set_read_timeout(Some(timeout));

    debug!("Starting consumer request to: {:?}", url);
    client.get(url)
          .send()
          .map_err(|req_err| {
              error!("Request error: {:?}", req_err);
              format!("Request Error: {}", req_err)
          })
          .and_then(|mut response| {
              debug!("Got response for Consumer");
              let stop_result = process_streaming_response(consumer, &mut response);
              debug!("finished running consumer with result: {:?}", stop_result);
              stop_result
          })
}

fn process_streaming_response<T: FloConsumer>(consumer: &mut T, response: &mut Response) -> StopResult {
    use serde_json::de::StreamDeserializer;

    let event_iter = StreamDeserializer::new(response.bytes()).map(|json_result| json_result.map(Event::from_complete_json));

    let mut result = Ok(());
    for event_result in event_iter {
        match event_result {
            Ok(event) => {
                debug!("Calling consumer with event: {}", event.get_id());
                if let ConsumerCommand::Stop(stop_result) = consumer.on_event(event) {
                    result = stop_result;
                    break;
                }
            }
            Err(serde_error) => {
                error!("got serde error: {:?}", serde_error);
                result = Err(format!("serde_error: {:?}", serde_error));
                break;
            }
        }
    }

    result
}
