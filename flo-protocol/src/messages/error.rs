
use nom::be_u32;
use serializer::Serializer;
use event::OwnedFloEvent;
use super::{ProtocolMessage, parse_str};

pub const ERROR_HEADER: u8 = 10;


pub const ERROR_INVALID_NAMESPACE: u8 = 15;
pub const ERROR_INVALID_CONSUMER_STATE: u8 = 16;
pub const ERROR_INVALID_VERSION_VECTOR: u8 = 17;
pub const ERROR_STORAGE_ENGINE_IO: u8 = 18;
pub const ERROR_NO_STREAM: u8 = 19;

/// Describes the type of error. This gets serialized a u8
#[derive(Debug, PartialEq, Clone)]
pub enum ErrorKind {
    /// Indicates that the namespace provided by a consumer was an invalid glob pattern
    InvalidNamespaceGlob,
    /// Indicates that the client connection was in an invalid state when it attempted some consumer operation
    InvalidConsumerState,
    /// Indicates that the provided version vector was invalid (contained more than one entry for at least one actor id)
    InvalidVersionVector,
    /// Unable to read or write to events file
    StorageEngineError,
    /// Requested event stream does not exist
    NoSuchStream,
}

/// Represents a response to any request that results in an error
#[derive(Debug, PartialEq, Clone)]
pub struct ErrorMessage {
    /// The op_id of the request to make it easier to correlate request/response pairs
    pub op_id: u32,

    /// The type of error
    pub kind: ErrorKind,

    /// A human-readable description of the error
    pub description: String,
}

impl ErrorKind {
    /// Converts from the serialized u8 to an ErrorKind
    pub fn from_u8(byte: u8) -> Result<ErrorKind, u8> {
        match byte {
            ERROR_INVALID_NAMESPACE => Ok(ErrorKind::InvalidNamespaceGlob),
            ERROR_INVALID_CONSUMER_STATE => Ok(ErrorKind::InvalidConsumerState),
            ERROR_INVALID_VERSION_VECTOR => Ok(ErrorKind::InvalidVersionVector),
            ERROR_STORAGE_ENGINE_IO => Ok(ErrorKind::StorageEngineError),
            ERROR_NO_STREAM => Ok(ErrorKind::NoSuchStream),
            other => Err(other)
        }
    }

    /// Converts the ErrorKind to it's serialized u8 value
    pub fn u8_value(&self) -> u8 {
        match self {
            &ErrorKind::InvalidNamespaceGlob => ERROR_INVALID_NAMESPACE,
            &ErrorKind::InvalidConsumerState => ERROR_INVALID_CONSUMER_STATE,
            &ErrorKind::InvalidVersionVector => ERROR_INVALID_VERSION_VECTOR,
            &ErrorKind::StorageEngineError => ERROR_STORAGE_ENGINE_IO,
            &ErrorKind::NoSuchStream => ERROR_NO_STREAM,
        }
    }
}


named!{pub parse_error_message<ProtocolMessage<OwnedFloEvent>>,
    chain!(
        _tag: tag!(&[ERROR_HEADER]) ~
        op_id: be_u32 ~
        kind: map_res!(take!(1), |res: &[u8]| {
            ErrorKind::from_u8(res[0])
        }) ~
        description: parse_str,
        || {
            ProtocolMessage::Error(ErrorMessage {
                op_id: op_id,
                kind: kind,
                description: description,
            })
        }
    )
}

pub fn serialize_error_message(err: &ErrorMessage, buf: &mut [u8]) -> usize {
    Serializer::new(buf).write_u8(ERROR_HEADER)
                        .write_u32(err.op_id)
                        .write_u8(err.kind.u8_value())
                        .write_string(&err.description)
                        .finish()
}
