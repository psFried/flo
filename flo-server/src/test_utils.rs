
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use futures::executor::{spawn, Unpark};
use futures::{Future, Async};

pub fn addr(string: &str) -> SocketAddr {
    ::std::str::FromStr::from_str(string).unwrap()
}

/// used for testing futures when we don't expect the future to need unparked
struct NoOpUnpark;
impl Unpark for NoOpUnpark {
    fn unpark(&self) {
        unimplemented!()
    }
}

pub fn expect_future_resolved<F, T, E>(future: F) -> Result<T, E> where F: Future<Item=T, Error=E> {
    let mut s = spawn(future);
    let unpark = ::std::sync::Arc::new(NoOpUnpark);
    loop {
        let result = s.poll_future(unpark.clone());
        match result {
            Ok(Async::Ready(t)) => {
                return Ok(t);
            }
            Err(e) => {
                return Err(e);
            }
            Ok(Async::NotReady) => {
                panic!("Future was not ready");
            }
        }
    }
}
