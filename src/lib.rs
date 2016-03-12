extern crate hyper;

use hyper::Server;
use hyper::server::Request;
use hyper::server::Response;

fn handle_request(_req: Request, response: Response) {
    response.send(b"hello!").unwrap();
}

pub fn start_server() {
    Server::http("0.0.0.0:4444").unwrap().handle(handle_request).unwrap();
}
