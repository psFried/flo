use rotor::{Scope, Time};
use rotor_http::server::{Response, Version};
use std::time::Duration;

use netbuf::Buf;
use context::FloContext;


pub fn timeout(now: Time) -> Time {
    now + Duration::new(15, 0)
}

pub fn handle_request(data: &[u8], res: &mut Response, context: &mut FloContext) {
    let event = ::std::str::from_utf8(data).ok()
            .map(|as_str| {
                let mut as_string = as_str.to_string();
                as_string.push_str("\r\n");
                as_string
            }).unwrap();
    println!("Adding event: {:?}", event);

    let body = format!("Added event: {}", event);
    res.status(200u16, "Success");
    res.add_chunked().unwrap();
    res.done_headers().unwrap();
    res.write_body(body.as_bytes());
    res.done();
    context.add_event(event);

    context.notify_all_consumers();
}

#[cfg(test)]
mod test {
    use super::*;
    use rotor::{Scope, Time};
    use rotor_http::server::{Response, Version};
    use std::time::Duration;

    use netbuf::Buf;

    #[test]
    fn handle_request_notifies_all_consumers() {
        let mut response_buffer = Buf::new();
        let mut response = mock_response(&mut response_buffer);



    }



    fn mock_response<'a>(response_buffer: &'a mut Buf) -> Response<'a> {
        Response::new(response_buffer, Version::Http11, false, true)
    }
}
