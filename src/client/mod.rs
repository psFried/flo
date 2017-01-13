pub mod sync;

use std::io;
use protocol::{ProtocolMessage, ServerMessage, EventHeader};
use flo_event::{FloEventId, OwnedFloEvent};


#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    UnexpectedMessage(ServerMessage<OwnedFloEvent>),
}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        ClientError::Io(err)
    }
}


#[derive(Debug, PartialEq, Clone)]
pub struct ConsumerOptions {
    pub namespace: String,
    pub start_position: Option<FloEventId>,
    pub max_events: u64,
    pub username: String,
    pub password: String,
}



