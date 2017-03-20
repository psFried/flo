#[cfg(feature = "serde-json-codec")]
mod serde;

use std::string::FromUtf8Error;
use std::error::Error;

#[cfg(feature = "serde-json-codec")]
pub use self::serde::{SerdeJsonCodec, SerdePrettyJsonCodec};

/// Trait that allows events to be converted to application-specific types. An `EventCodec` is associated with a
/// connection, and is used to convert all incoming and outgoing events. Note that the types that are produced and consumed
/// may be different.
pub trait EventCodec<B> {
    type Error: Error + 'static;
    fn convert_received(&self, namespace: &str, data: Vec<u8>) -> Result<B, Self::Error>;
    fn convert_produced(&self, namespace: &str, data: B) -> Result<Vec<u8>, Self::Error>;
}

/// The simplest possible codec. It just passes every event through as it is and only produces binary data.
pub struct RawCodec;
impl EventCodec<Vec<u8>> for RawCodec {
    type Error = ImpossibleError;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<Vec<u8>, ImpossibleError> {
        Ok(data)
    }

    fn convert_produced(&self, _namespace: &str, output: Vec<u8>) -> Result<Vec<u8>, ImpossibleError> {
        Ok(output)
    }
}

/// An EventCodec for events that use UTF-8 Strings for the event body. Clients using this codec will receive
/// `BasicEvent<String>` instances in the consumer, and will produce simple `String`s. This codec will return an error
/// if the event data is not valid UTF-8. If this isn't what you want, then consider using the `LossyStringCodec` instead.
pub struct StringCodec;
impl EventCodec<String> for StringCodec {
    type Error = FromUtf8Error;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<String, FromUtf8Error> {
        String::from_utf8(data)
    }

    fn convert_produced(&self, _namespace: &str, output: String) -> Result<Vec<u8>, FromUtf8Error> {
        Ok(output.into_bytes())
    }
}

/// A more permissive version of the `StringCodec` that will allow non-utf8 characters by converting them into � characters.
pub struct LossyStringCodec;
impl EventCodec<String> for LossyStringCodec {
    type Error = ImpossibleError;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<String, ImpossibleError> {
        Ok(String::from_utf8_lossy(&data).into_owned())
    }

    fn convert_produced(&self, _namespace: &str, output: String) -> Result<Vec<u8>, ImpossibleError> {
        Ok(output.into_bytes())
    }
}

/// This error can never actually be instantiated. It only exists to satisfy the type checker.
#[doc(hidden)]
#[derive(Debug)]
pub struct ImpossibleError;

impl ::std::fmt::Display for ImpossibleError {
    fn fmt(&self, _f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        unimplemented!()
    }
}

impl ::std::error::Error for ImpossibleError {
    fn description(&self) -> &str {
        unimplemented!()
    }
}
