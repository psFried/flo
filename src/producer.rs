use rotor::{Scope, Time};
use rotor_http::server::{Response, Version};
use serde_json::de::from_slice;
use serde_json::Value;
use serde_json::error::Error;
use netbuf::Buf;

use context::FloContext;
use consumer::FloConsumer;
use event_store::EventStore;

use std::time::Duration;
use std::io::Write;


pub fn timeout(now: Time) -> Time {
    now + Duration::new(15, 0)
}

pub fn handle_request<C, S>(data: &[u8], res: &mut Response, context: &mut FloContext<C, S>)
        where C: FloConsumer, S: EventStore {

    match from_slice(data) {
        Ok(event) => {
            let body = format!("Added event: {:?}", event);
            write_response(200u16, &body, res);
            context.add_event(event);
        },
        _ => {
            write_response(400u16, "invalid json", res);
        }
    }

}

fn write_response(status: u16, body: &str, res: &mut Response) {
    res.status(status, "Success");
    res.add_length(body.len() as u64).unwrap();
    res.done_headers().unwrap();
    res.write_body(body.as_bytes());
    res.done();
}

#[cfg(test)]
mod test {
    use super::*;
    use rotor::{Scope, Time};
    use rotor_http::server::{Response, Version};
    use std::time::Duration;
    use netbuf::Buf;
    use context::FloContext;
    use test_utils::{self, assert_http_status, assert_response_body, MockConsumer};
    use std::io::Write;


    macro_rules! test_request {
        ( $request_body:expr) => {
            {
                let mut buf = Buf::new();
                {
                    let mut response = Response::new(&mut buf, Version::Http11, false, false);
                    let mut ctx = test_utils::mock_flo_context();
                    handle_request($request_body, &mut response, &mut ctx);
                }
                let mut response_data = Vec::new();
                buf.write_to(&mut response_data);
                response_data
            }
        }
    }

    #[test]
    fn handle_request_sends_error_if_event_is_not_valid_json() {
        let mut response_buf = test_request!(b"lksdfjk");
        assert_http_status(400u16, response_buf.as_slice());
    }

    #[test]
    fn handle_request_sends_body_with_success_message() {
        let mut response_data = test_request!(b"{\"anyKey\": \"anyValue\"}");
        assert_response_body("Added event: {\"anyKey\":\"anyValue\"}", response_data.as_slice());
    }

    #[test]
    fn handle_request_returns_status_200_if_event_was_valid_json() {
        let mut response_data = test_request!(b"{\"anyKey\": \"anyValue\"}");
        assert_http_status(200u16, response_data.as_slice());
    }

}
