
use std::net::SocketAddr;

pub fn addr(string: &str) -> SocketAddr {
    ::std::str::FromStr::from_str(string).unwrap()
}
