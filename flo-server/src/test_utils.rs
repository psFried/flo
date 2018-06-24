
use std::net::SocketAddr;
use std::sync::{Once, ONCE_INIT};

use futures::executor::{spawn, Notify};
use futures::{Future, Async};

static LOGGER_INIT: Once = ONCE_INIT;

pub fn init_logging() {
    LOGGER_INIT.call_once(|| {
        let _ = ::env_logger::init();
    });
}

pub fn addr(string: &str) -> SocketAddr {
    ::std::str::FromStr::from_str(string).unwrap()
}

/// used for testing futures when we don't expect the future to need Notified
struct NoOpNotify;
impl Notify for NoOpNotify {
    fn notify(&self, _id: usize) {
        panic!("Expected that this future should not be notified");
    }
}

pub fn expect_future_resolved<F, T, E>(future: F) -> Result<T, E> where F: Future<Item=T, Error=E> {
    let mut s = spawn(future);
    let unpark = ::std::sync::Arc::new(NoOpNotify);
    loop {
        let result = s.poll_future_notify(&unpark, 0);
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
