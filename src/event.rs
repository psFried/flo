use serde_json::{self, Value, builder};
use std::io::Read;

pub type Json = Value;
pub type EventId = u64;
pub type ParseResult<T> = Result<T, serde_json::Error>;

const ID_KEY: &'static str = "id";
const DATA_KEY: &'static str = "data";

#[derive(Debug, PartialEq, Clone)]
pub struct Event {
    pub data: Value,
}

impl Event {

    pub fn new(id: EventId, data: Json) -> Event {
        let data_with_id = builder::ObjectBuilder::new()
                .insert(ID_KEY, id)
                .insert(DATA_KEY, data)
                .unwrap();
        Event {
            data: data_with_id,
        }
    }

    pub fn from_complete_json(json: Json) -> Event {
        Event {
            data: json
        }
    }

    pub fn from_reader<R: Read>(raw_data: R) -> ParseResult<Event> {
        serde_json::de::from_reader(raw_data)
                .map(Event::from_complete_json)
    }

    pub fn from_slice(complete_data: &[u8]) -> ParseResult<Event> {
        serde_json::de::from_slice(complete_data)
                .map(Event::from_complete_json)
    }

    pub fn from_str(complete_data: &str) -> ParseResult<Event> {
        serde_json::de::from_str(complete_data)
                .map(Event::from_complete_json)
    }

    pub fn get_id(&self) -> EventId {
        self.data.find(ID_KEY).unwrap().as_u64().unwrap()
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

#[cfg(test)]
mod test {
    use super::*;

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
