use std::string::FromUtf8Error;
use std::error::Error;

use event::{OwnedFloEvent, FloEventId, Timestamp};

/// Trait that allows events to be converted to application-specific types. An `EventCodec` is associated with a
/// connection, and is used to convert all incoming and outgoing events. Note that the types that are produced and consumed
/// may be different.
pub trait EventCodec {
    type Body;
    type Error: Error;

    fn convert_received(&self, namespace: &str, data: Vec<u8>) -> Result<Self::Body, Self::Error>;
    fn convert_produced(&self, namespace: &str, data: Self::Body) -> Result<Vec<u8>, Self::Error>;
}

/// The simplest possible codec. It just passes every event through as it is and only produces binary data.
pub struct RawCodec;
impl EventCodec for RawCodec {
    type Body = Vec<u8>;
    type Error = ImpossibleError;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<Self::Body, Self::Error> {
        Ok(data)
    }

    fn convert_produced(&self, _namespace: &str, output: Self::Body) -> Result<Vec<u8>, Self::Error> {
        Ok(output)
    }
}

/// An EventCodec for events that use UTF-8 Strings for the event body. Clients using this codec will receive
/// `BasicEvent<String>` instances in the consumer, and will produce simple `String`s. This codec will return an error
/// if the event data is not valid UTF-8. If this isn't what you want, then consider using the `LossyStringCodec` instead.
pub struct StringCodec;
impl EventCodec for StringCodec {
    type Body = String;
    type Error = FromUtf8Error;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<Self::Body, Self::Error> {
        String::from_utf8(data)
    }

    fn convert_produced(&self, _namespace: &str, output: Self::Body) -> Result<Vec<u8>, Self::Error> {
        Ok(output.into_bytes())
    }
}

/// A more permissive version of the `StringCodec` that will allow non-utf8 characters by converting them into � characters.
pub struct LossyStringCodec;
impl EventCodec for LossyStringCodec {
    type Body = String;
    type Error = ImpossibleError;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<Self::Body, Self::Error> {
        Ok(String::from_utf8_lossy(&data).into_owned())
    }

    fn convert_produced(&self, _namespace: &str, output: Self::Body) -> Result<Vec<u8>, Self::Error> {
        Ok(output.into_bytes())
    }
}

/// This error can never actually be instantiated. It only exists to satisfy the type checker.
#[doc(hidden)]
#[derive(Debug)]
pub struct ImpossibleError;

impl ::std::fmt::Display for ImpossibleError {
    fn fmt(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        unimplemented!()
    }
}

impl ::std::error::Error for ImpossibleError {
    fn description(&self) -> &str {
        unimplemented!()
    }
}
