#[cfg(feature = "serde-json-codec")]
mod serde;

use std::string::FromUtf8Error;
use std::error::Error;

use event::OwnedFloEvent;
use ::Event;

#[cfg(feature = "serde-json-codec")]
pub use self::serde::{SerdeJsonCodec, SerdePrettyJsonCodec};

/// Trait that allows events to be converted to application-specific types. An `EventCodec` is associated with a
/// connection, and is used to convert all incoming and outgoing events. Note that the types that are produced and consumed
/// may be different.
pub trait EventCodec {
    type EventData;
    fn convert_received(&self, namespace: &str, data: Vec<u8>) -> Result<Self::EventData, Box<Error>>;
    fn convert_produced(&self, namespace: &str, data: Self::EventData) -> Result<Vec<u8>, Box<Error>>;

    fn convert_from_message(&self, input: OwnedFloEvent) -> Result<Event<Self::EventData>, Box<Error>> {
        let OwnedFloEvent{id, parent_id, namespace, timestamp, data} = input;
        let converted = {
            self.convert_received(&namespace, data)
        };

        converted.map(move |body| {
            Event{
                id: id,
                parent_id: parent_id,
                timestamp: timestamp,
                namespace: namespace,
                data: body,
            }
        })
    }
}

/// The simplest possible codec. It just passes every event through as it is and only produces binary data.
pub struct RawCodec;
impl EventCodec for RawCodec {
    type EventData = Vec<u8>;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<Vec<u8>, Box<Error>> {
        Ok(data)
    }

    fn convert_produced(&self, _namespace: &str, output: Vec<u8>) -> Result<Vec<u8>, Box<Error>> {
        Ok(output)
    }
}

/// An EventCodec for events that use UTF-8 Strings for the event body. Clients using this codec will receive
/// `BasicEvent<String>` instances in the consumer, and will produce simple `String`s. This codec will return an error
/// if the event data is not valid UTF-8. If this isn't what you want, then consider using the `LossyStringCodec` instead.
pub struct StringCodec;
impl EventCodec for StringCodec {
    type EventData = String;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<String, Box<Error>> {
        String::from_utf8(data).map_err(|e| Box::new(e) as Box<Error>)
    }

    fn convert_produced(&self, _namespace: &str, output: String) -> Result<Vec<u8>, Box<Error>> {
        Ok(output.into_bytes())
    }
}

/// A more permissive version of the `StringCodec` that will allow non-utf8 characters by converting them into � characters.
pub struct LossyStringCodec;
impl EventCodec for LossyStringCodec {
    type EventData = String;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<String, Box<Error>> {
        Ok(String::from_utf8_lossy(&data).into_owned())
    }

    fn convert_produced(&self, _namespace: &str, output: String) -> Result<Vec<u8>, Box<Error>> {
        Ok(output.into_bytes())
    }
}
