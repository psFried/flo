use super::EventCodec;

use std::fmt::{self, Display, Formatter};
use std::error::Error;

use serde::{Serialize, Deserialize, Deserializer, Serializer};
use serde_json::{Serializer as JsonSerializer, Deserializer as JsonDeserializer};
use serde_json::error::Error as JsonError;
use serde_json::ser::{CompactFormatter, PrettyFormatter};

/// A codec that uses the serde_json library to automatically convert types that implement `serde::Serialize`
/// and `serde::Deserialize`. It's very common for those traits to simply be derived, making it really easy to
/// use native rust types as event bodies. 
pub struct SerdeJsonCodec;

impl <T> EventCodec<T> for SerdeJsonCodec where T: Serialize + Deserialize {
    type Error = JsonError;

    fn convert_received(&self, namespace: &str, data: Vec<u8>) -> Result<T, Self::Error> {
        let mut deser = JsonDeserializer::from_slice(&data);
        T::deserialize(&mut deser)
    }

    fn convert_produced(&self, namespace: &str, data: T) -> Result<Vec<u8>, Self::Error> {
        let mut serializer = JsonSerializer::new(Vec::new());
        data.serialize(&mut serializer).map(|()| serializer.into_inner())
    }
}

/// Just like the `SerdeJsonCodec`, except that it pretty-prints the json for produced events.
pub struct SerdePrettyJsonCodec;

impl <T> EventCodec<T> for SerdePrettyJsonCodec where T: Serialize + Deserialize {
    type Error = JsonError;

    fn convert_received(&self, namespace: &str, data: Vec<u8>) -> Result<T, Self::Error> {
        let mut deser = JsonDeserializer::from_slice(&data);
        T::deserialize(&mut deser)
    }

    fn convert_produced(&self, namespace: &str, data: T) -> Result<Vec<u8>, Self::Error> {
        let mut serializer = JsonSerializer::pretty(Vec::new());
        data.serialize(&mut serializer).map(|()| serializer.into_inner())
    }
}


