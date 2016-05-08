use rotor::Time;
use rotor_http::server::Response;
use serde_json::de::from_slice;

use context::FloContext;
use server::consumer::ConsumerNotifier;
use event_store::EventStore;

use std::time::Duration;


pub fn timeout(now: Time) -> Time {
    now + Duration::new(15, 0)
}

pub fn handle_request<C, S>(data: &[u8], res: &mut Response, context: &mut FloContext<C, S>)
        where C: ConsumerNotifier, S: EventStore {

    match from_slice(data) {
        Ok(event) => {
            let body = format!("Added event: {:?}", event);
            match context.add_event(event) {
                Ok(_) => write_response(200u16, &body, res),
                Err(_) => write_response(500, "oh shit man", res)
            }
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
    use rotor_http::server::{Response, Version};
    use netbuf::Buf;
    use test_utils::{self, assert_http_status, assert_response_body};

    macro_rules! test_request {
        ( $request_body:expr) => {
            {
                let mut buf = Buf::new();
                {
                    let mut response = Response::new(&mut buf, Version::Http11, false, false);
                    let mut ctx = test_utils::create_test_flo_context();
                    handle_request($request_body, &mut response, &mut ctx);
                }
                let mut response_data = Vec::new();
                buf.write_to(&mut response_data).unwrap();
                response_data
            }
        }
    }

    #[test]
    fn handle_request_sends_error_if_event_is_not_valid_json() {
        let response_buf = test_request!(b"lksdfjk");
        assert_http_status(400u16, response_buf.as_slice());
    }

    #[test]
    fn handle_request_sends_body_with_success_message() {
        let response_data = test_request!(b"{\"anyKey\": \"anyValue\"}");
        assert_response_body("Added event: {\"anyKey\":\"anyValue\"}", response_data.as_slice());
    }

    #[test]
    fn handle_request_returns_status_200_if_event_was_valid_json() {
        let response_data = test_request!(b"{\"anyKey\": \"anyValue\"}");
        assert_http_status(200u16, response_data.as_slice());
    }

}
