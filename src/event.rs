use std::io::{self, Read};
use std::string::FromUtf8Error;
use serde_json::{self, Value, builder, Deserializer};

pub use serde_json::{Error, ErrorCode};
pub use serde_json::builder::ObjectBuilder;

pub type Json = Value;
pub type EventId = u64;
pub type ParseResult<T> = Result<T, serde_json::Error>;

const ID_KEY: &'static str = "id";
const DATA_KEY: &'static str = "data";

#[derive(Debug, Clone)]
pub struct Event {
    pub data: Value,
    raw_bytes: Option<Vec<u8>>,
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Event {

    pub fn new(id: EventId, data: Json) -> Event {
        let data_with_id = builder::ObjectBuilder::new()
                .insert(ID_KEY, id)
                .insert(DATA_KEY, data)
                .unwrap();
        Event {
            data: data_with_id,
            raw_bytes: None,
        }
    }

    pub fn get_raw_bytes(&mut self) -> &[u8] {
        match self.raw_bytes {
            Some(ref bytes) => bytes,
            None => {
                self.raw_bytes = Some(serde_json::to_vec(&self.data).unwrap());
                self.get_raw_bytes()
            }
        }
    }

    pub fn get_id(&self) -> EventId {
        self.data.find(ID_KEY).unwrap().as_u64().unwrap()
    }

    pub fn from_complete_json(json: Json) -> Event {
        Event {
            data: json,
            raw_bytes: None,
        }
    }

    pub fn from_slice(complete_data: &[u8]) -> ParseResult<Event> {
        serde_json::de::from_slice(complete_data)
                .map(Event::from_complete_json)
    }


    #[cfg(test)]
    pub fn from_reader<R: Read>(raw_data: R) -> ParseResult<Event> {
        serde_json::de::from_reader(raw_data)
                .map(Event::from_complete_json)
    }

    #[cfg(test)]
    pub fn from_str(complete_data: &str) -> ParseResult<Event> {
        serde_json::de::from_str(complete_data)
                .map(Event::from_complete_json)
    }

}



pub fn to_event(id: EventId, json: &str) -> ParseResult<Event> {
    to_json(json).map(|data| {
        Event::new(id, data)
    })
}

pub fn to_json(json: &str) -> ParseResult<Json> {
    serde_json::from_str(json)
}

pub fn to_bytes(json: &Json) -> ParseResult<Vec<u8>> {
    use serde_json::ser::to_vec;

    to_vec(json)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn event_is_equal_to_another_event_if_json_is_equal() {
        let mut evt1 = Event::from_str(r#"{"data":{"someKey":"someValue"},"id":234}"#).unwrap();
        let mut evt2 = Event::from_str(r#"{"data":{"someKey":"someValue"},"id":234}"#).unwrap();
        assert_eq!(evt1, evt2);

        evt1.raw_bytes = Some(Vec::new());
        assert_eq!(evt1, evt2);
    }

    #[test]
    fn event_caches_its_raw_data_when_get_raw_bytes_is_called() {
        let mut event = Event::from_str(r#"{"data":{"someKey":"someValue"},"id":234}"#).unwrap();
        assert!(event.raw_bytes.is_none());

        event.get_raw_bytes();
        assert!(event.raw_bytes.is_some());
    }

    #[test]
    fn event_is_serialized_to_a_byte_array() {
        let event_data = r#"{"data":{"someKey":"someValue"},"id":234}"#;
        let mut event = Event::from_str(event_data).unwrap();
        let result = event.get_raw_bytes();
        assert_eq!(event_data.as_bytes(), result);
    }

    #[test]
    fn client_event_data_is_stored_under_the_data_key() {
        let data = to_json(r#"{"boo": "ya"}"#).unwrap();

        let event = Event::new(0, data.clone());
        assert_eq!(Some(&data), event.data.find("data"));
    }

    #[test]
    fn creating_an_event_adds_the_id() {
        let id: EventId = 6;
        let event = to_event(id, r#"{"boo": "ya"}"#).unwrap();

        assert_eq!(id, event.get_id());
    }
}
