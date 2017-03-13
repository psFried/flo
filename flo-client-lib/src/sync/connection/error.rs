use std::io;
use protocol::{ErrorMessage, ProtocolMessage};


#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    FloError(ErrorMessage),
    UnexpectedMessage(ProtocolMessage),
    EndOfStream,
}

impl ClientError {
    pub fn is_timeout(&self) -> bool {
        match *self {
            ClientError::Io(ref io_err) if io_err.kind() == io::ErrorKind::WouldBlock => true,
            _ => false
        }
    }

    pub fn is_end_of_stream(&self) -> bool {
        if let ClientError::EndOfStream = *self {
            true
        } else {
            false
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        ClientError::Io(err)
    }
}

impl From<ErrorMessage> for ClientError {
    fn from(err: ErrorMessage) -> Self {
        ClientError::FloError(err)
    }
}
