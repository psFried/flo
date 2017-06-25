use super::EventCodec;

use serde::{Serialize, Deserialize};
use serde_json::{Serializer as JsonSerializer, Deserializer as JsonDeserializer};
use serde_json::error::Error as JsonError;
use std::marker::PhantomData;

/// A codec that uses the serde_json library to automatically convert types that implement `serde::Serialize`
/// and `serde::Deserialize`. It's very common for those traits to simply be derived, making it really easy to
/// use native rust types as event bodies. 
pub struct SerdeJsonCodec<T: Serialize + Deserialize>{
    _phantom_data: PhantomData<T>,
}
impl <T: Serialize + Deserialize> SerdeJsonCodec<T> {
    pub fn new() -> SerdeJsonCodec<T> {
        SerdeJsonCodec {
            _phantom_data: PhantomData
        }
    }
}

impl <T> EventCodec for SerdeJsonCodec<T> where T: Serialize + Deserialize {
    type EventData = T;
    type Error = JsonError;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<T, Self::Error> {
        let mut deser = JsonDeserializer::from_slice(&data);
        T::deserialize(&mut deser)
    }

    fn convert_produced(&self, _namespace: &str, data: T) -> Result<Vec<u8>, Self::Error> {
        let mut serializer = JsonSerializer::new(Vec::new());
        data.serialize(&mut serializer).map(|()| serializer.into_inner())
    }
}

/// Just like the `SerdeJsonCodec`, except that it pretty-prints the json for produced events.
pub struct SerdePrettyJsonCodec<T: Serialize + Deserialize>{
    _phantom_data: PhantomData<T>,
}

impl <T: Serialize + Deserialize> SerdePrettyJsonCodec<T> {
    pub fn new() -> SerdePrettyJsonCodec<T> {
        SerdePrettyJsonCodec {
            _phantom_data: PhantomData
        }
    }
}

impl <T> EventCodec for SerdePrettyJsonCodec<T> where T: Serialize + Deserialize {
    type EventData = T;
    type Error = JsonError;

    fn convert_received(&self, _namespace: &str, data: Vec<u8>) -> Result<T, Self::Error> {
        let mut deser = JsonDeserializer::from_slice(&data);
        T::deserialize(&mut deser)
    }

    fn convert_produced(&self, _namespace: &str, data: T) -> Result<Vec<u8>, Self::Error> {
        let mut serializer = JsonSerializer::pretty(Vec::new());
        data.serialize(&mut serializer).map(|()| serializer.into_inner())
    }
}


