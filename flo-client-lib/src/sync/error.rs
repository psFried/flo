use std::io;
use protocol::ProtocolMessage;
use std::fmt::{self, Display, Formatter};
use std::error::Error;

pub use protocol::ErrorMessage;

/// Describes all the possible errors that can happen between the client and the server
#[derive(Debug)]
pub enum ClientError {
    /// An error message that was returned by the server. Often times, this simple represents the client making some sort of
    /// invalid request, although some `ErrorMessage` variants indicate issues with the server.
    FloError(ErrorMessage),

    /// An unexpected message was received by the client. This most likely indicates a bug in either the client or the server
    UnexpectedMessage(ProtocolMessage),

    /// An error that occurred within the `EventCodec`. This is likely a deserialization error.
    Codec(Box<Error>),

    /// Transport errors cover the range of io errors that can happen while sending messages between the client and server.
    Transport(io::Error)
}

impl ClientError {
    pub fn is_timeout(&self) -> bool {
        match *self {
            ClientError::Transport(ref err) if err.kind() == io::ErrorKind::WouldBlock => true,
            _ => false
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
