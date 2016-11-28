use std::cmp::{Ord, PartialOrd, Ordering};


pub type ActorId = u16;
pub type EventCounter = u64;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct FloEventId {
    pub actor: ActorId,
    pub event_counter: EventCounter,
}

impl FloEventId {
    pub fn new(actor: ActorId, event_counter: EventCounter) -> FloEventId {
        FloEventId {
            actor: actor,
            event_counter: event_counter,
        }
    }
}

impl Ord for FloEventId {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.event_counter == other.event_counter {
            self.actor.cmp(&other.actor)
        } else {
            self.event_counter.cmp(&other.event_counter)
        }
    }
}

impl PartialOrd for FloEventId {
    fn partial_cmp(&self, other: &FloEventId) -> Option<Ordering> {
        if self.event_counter == other.event_counter {
            self.actor.partial_cmp(&other.actor)
        } else {
            self.event_counter.partial_cmp(&other.event_counter)
        }
    }
}

pub trait FloEvent {
    fn id(&self) -> &FloEventId;
    fn namespace(&self) -> &str;
    fn data_len(&self) -> u32;
    fn data(&self) -> &[u8];

    fn to_owned(&self) -> OwnedFloEvent;
}

#[derive(Debug, PartialEq, Clone)]
pub struct OwnedFloEvent {
    pub id: FloEventId,
    pub namespace: String,
    pub data: Vec<u8>,
}

impl OwnedFloEvent {
    pub fn new(id: FloEventId, namespace: String, data: Vec<u8>) -> OwnedFloEvent {
        OwnedFloEvent {
            id: id,
            namespace: namespace,
            data: data,
        }
    }
}

impl FloEvent for OwnedFloEvent {
    fn id(&self) -> &FloEventId {
        &self.id
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn data_len(&self) -> u32 {
        self.data.len() as u32
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn to_owned(&self) -> OwnedFloEvent {
        self.clone()
    }
}
