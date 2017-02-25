
use futures::sync::mpsc as fchannels;
use std::sync::mpsc as stdchannels;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct SendError<T: Sized>(T);
impl <T> SendError<T> {
    #[allow(dead_code)]
    pub fn into_message(self) -> T {
        self.0
    }
}
impl <T> Error for SendError<T> where T: fmt::Debug {
    fn description(&self) -> &str {
        "Unable to send message through the channel because the receiver was already dropped"
    }
}

impl <T> fmt::Display for SendError<T> where T: fmt::Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error sending message: {:?}", self.0)
    }
}

impl <T> From<fchannels::SendError<T>> for SendError<T> {
    fn from(err: fchannels::SendError<T>) -> Self {
        SendError(err.into_inner())
    }
}

impl <T> From<stdchannels::SendError<T>> for SendError<T> {
    fn from(err: stdchannels::SendError<T>) -> Self {
        SendError(err.0)
    }
}

pub trait Sender<T> {
    fn send(&self, message: T) -> Result<(), SendError<T>>;
}

impl <T> Sender<T> for fchannels::UnboundedSender<T> where T: Send {
    fn send(&self, message: T) -> Result<(), SendError<T>> {
        self.send(message).map_err(|err| err.into())
    }
}

impl <T> Sender<T> for stdchannels::Sender<T> where T: Send {
    fn send(&self, message: T) -> Result<(), SendError<T>> {
        self.send(message).map_err(|err| err.into())
    }
}

#[cfg(test)]
pub use self::test_util::MockSender;

#[cfg(test)]
#[allow(dead_code)]
mod test_util {
    use super::*;
    use std::sync::{Arc, Mutex};

    pub struct MockSender<T> {
        sent_values: Arc<Mutex<Vec<T>>>
    }

    impl <T> MockSender<T> {
        pub fn new() -> MockSender<T> {
            MockSender {
                sent_values: Arc::new(Mutex::new(Vec::new()))
            }
        }

        pub fn assert_message_sent(&mut self, expected: T) where T: PartialEq + ::std::fmt::Debug {
            let value = {
                let mut values = self.sent_values.lock().unwrap();
                values.pop() //make sure we drop the mutex lock here so we don't poison the lock if the assert fails
            };
            match value {
                Some(message) => {
                    assert_eq!(expected, message);
                }
                None => {
                    panic!("Expected message: {:?}, but no messages were sent")
                }
            }
        }

        pub fn get_sent_messages(&mut self) -> Vec<T> {
            let mut values = self.sent_values.lock().unwrap();
            ::std::mem::replace(&mut values, Vec::new())
        }

    }

    impl <T> Sender<T> for MockSender<T> {
        fn send(&self, message: T) -> Result<(), SendError<T>> {
            let mut values = self.sent_values.lock().unwrap();
            values.push(message);
            Ok(())
        }
    }
}


