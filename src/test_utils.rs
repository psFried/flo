
use consumer::FloConsumer;
use rotor_http::server::{Response, Version};

use netbuf::Buf;
use httparse;

pub struct MockConsumer {
    pub notify_invokations: u32
}

impl MockConsumer {

    pub fn new() -> MockConsumer {
        MockConsumer {
            notify_invokations: 0
        }
    }
}

impl FloConsumer for MockConsumer {
    fn notify(&mut self) {
        self.notify_invokations += 1;
    }
}

pub fn assert_response_body(expected: &str, buffer: &[u8]) {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut response = httparse::Response::new(&mut headers);
    let parse_result = response.parse(buffer).unwrap();
    assert!(parse_result.is_complete());

    let buffer_position: usize = parse_result.unwrap();
    let (_, body) = buffer.split_at(buffer_position);
    let str_body = String::from_utf8_lossy(body);
    println!("str_body={}", str_body);
    assert_eq!(expected, str_body.trim());
}

pub fn assert_http_status(expected: u16, buffer: &[u8]) {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut response = httparse::Response::new(&mut headers);
    let parse_result = response.parse(buffer).unwrap();
    assert!(parse_result.is_complete());
    let actual_status = response.code.expect("Expected a response status code");
    assert_eq!(expected, actual_status);
}
