mod sync;

use std::io;
use protocol::{ProtocolMessage, ServerMessage, EventHeader};

pub use self::sync::SyncStream;

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    UnexpectedMessage(String),
}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        ClientError::Io(err)
    }
}

