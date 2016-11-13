#[cfg(test)]
use std::io::Read;

pub type EventId = u64;

const ID_KEY: &'static str = "id";
const DATA_KEY: &'static str = "data";

#[derive(Debug, Clone)]
pub struct Event {
    pub id: EventId,
    data: Vec<u8>
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Event {
    pub fn new(id: EventId, data: Vec<u8>) -> Event {
        Event {
            id: id,
            data: data,
        }
    }

    pub fn get_raw_bytes(&mut self) -> &[u8] {
        &self.data
    }

    pub fn get_id(&self) -> EventId {
        self.id
    }
}


