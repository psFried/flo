use std::io;
use protocol::{ErrorMessage, ProtocolMessage};
use std::fmt::{self, Display, Formatter};
use std::error::Error;

#[derive(Debug)]
pub enum ClientError {
    FloError(ErrorMessage),
    UnexpectedMessage(ProtocolMessage),
    EndOfStream,
    Codec(Box<Error>),
    Transport(io::Error)
}

impl ClientError {
    pub fn is_timeout(&self) -> bool {
        match *self {
            ClientError::Transport(ref err) if err.kind() == io::ErrorKind::WouldBlock => true,
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

impl From<ErrorMessage> for ClientError {
    fn from(err: ErrorMessage) -> Self {
        ClientError::FloError(err)
    }
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        ClientError::Transport(e)
    }
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.description())?;
        if let Some(cause) = self.cause() {
            write!(f, " (caused by: {})", cause)?;
        }
        Ok(())
    }
}


impl Error for ClientError {
    fn description(&self) -> &str {
        match *self {
            ClientError::EndOfStream => "End of Stream",
            ClientError::Codec(_) => "Codec Error",
            ClientError::FloError(_) => "Flo Error",
            ClientError::Transport(_) => "Transport Error",
            ClientError::UnexpectedMessage(_) => "Unexpected Message",
        }
    }

    fn cause(&self) -> Option<&Error> {
        use std::borrow::Borrow;

        match *self {
            ClientError::Transport(ref transport_err) => Some(transport_err.borrow() as &Error),
            ClientError::Codec(ref codec_err) => Some(codec_err.borrow()),
            _ => None
        }
    }
}
