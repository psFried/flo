use rotor::{Scope, Time};
use rotor_http::server::{Response, Version};
use serde_json::de::from_slice;
use serde_json::Value;
use netbuf::Buf;

use context::FloContext;
use consumer::FloConsumer;

use std::time::Duration;
use std::io::Write;


pub fn timeout(now: Time) -> Time {
    now + Duration::new(15, 0)
}

pub fn handle_request<C: FloConsumer>(data: &[u8], res: &mut Response, context: &mut FloContext<C>) {
    let event = ::std::str::from_utf8(data).ok()
            .map(|as_str| {
                let mut as_string = as_str.to_string();
                as_string.push_str("\r\n");
                as_string
            }).unwrap();
    println!("Adding event: {:?}", event);

    let body = format!("Added event: {}", event);
    write_response(200u16, &body, res);

    context.add_event(event);
    context.notify_all_consumers();
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
    use test_utils::{assert_http_status, assert_response_body, MockConsumer};
    use std::io::Write;

    #[test]
    fn handle_request_sends_body_with_success_message() {
        let mut buf = Buf::new();
        {
            let mut response = Response::new(&mut buf, Version::Http11, false, false);
            let mut ctx: FloContext<MockConsumer> = FloContext::new();

            let data = b"{anyKey: \"anyValue\"}";
            handle_request(data, &mut response, &mut ctx);
        }

        let mut response_data = Vec::new();
        buf.write_to(&mut response_data);

        assert_response_body("Added event: {anyKey: \"anyValue\"}", response_data.as_slice());
    }

    #[test]
    fn handle_request_returns_status_200_if_event_was_valid_json() {
        let mut buf = Buf::new();
        {
            let mut response = Response::new(&mut buf, Version::Http11, false, false);
            let mut ctx: FloContext<MockConsumer> = FloContext::new();

            let data = b"{anyKey: \"anyValue\"}";
            handle_request(data, &mut response, &mut ctx);
        }

        let mut response_data = Vec::new();
        buf.write_to(&mut response_data);

        assert_http_status(200u16, response_data.as_slice());
    }

    #[test]
    fn handle_request_notifies_all_consumers() {
        let mut buf = Buf::new();
        let mut response = Response::new(&mut buf, Version::Http11, false, false);
        let mut ctx: FloContext<MockConsumer> = FloContext::new();

        for _ in 0..3 {
            ctx.add_consumer(MockConsumer::new());
        }

        let data = b"{anyKey: \"anyValue\"}";

        handle_request(data, &mut response, &mut ctx);

        for consumer in ctx.consumers.iter() {
            assert_eq!(1, consumer.notify_invokations);
        }
    }


}
