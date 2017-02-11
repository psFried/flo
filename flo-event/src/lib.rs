extern crate chrono;

use std::cmp::{Ord, PartialOrd, Ordering};
use std::collections::HashMap;
use std::fmt::Debug;

use chrono::{DateTime, UTC};

pub type Timestamp = DateTime<UTC>;

pub type ActorId = u16;
pub type EventCounter = u64;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct FloEventId {
    pub actor: ActorId,
    pub event_counter: EventCounter,
}

pub const ZERO_EVENT_ID: FloEventId = FloEventId{event_counter: 0, actor: 0};

impl FloEventId {

    #[inline]
    pub fn zero() -> FloEventId {
        ZERO_EVENT_ID
    }

    pub fn new(actor: ActorId, event_counter: EventCounter) -> FloEventId {
        FloEventId {
            event_counter: event_counter,
            actor: actor,
        }
    }

    pub fn is_zero(&self) -> bool {
        *self == ZERO_EVENT_ID
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

pub trait FloEvent: Debug {
    fn id(&self) -> &FloEventId;
    fn timestamp(&self) -> Timestamp;
    fn parent_id(&self) -> Option<FloEventId>;
    fn namespace(&self) -> &str;
    fn data_len(&self) -> u32;
    fn data(&self) -> &[u8];

    fn to_owned(&self) -> OwnedFloEvent;
}

impl <T> FloEvent for T where T: AsRef<OwnedFloEvent> + Debug {
    fn id(&self) -> &FloEventId {
        self.as_ref().id()
    }

    fn namespace(&self) -> &str {
        self.as_ref().namespace()
    }

    fn data_len(&self) -> u32 {
        self.as_ref().data_len()
    }

    fn data(&self) -> &[u8] {
        self.as_ref().data()
    }

    fn to_owned(&self) -> OwnedFloEvent {
        self.as_ref().clone()
    }

    fn parent_id(&self) -> Option<FloEventId> {
        self.as_ref().parent_id()
    }

    fn timestamp(&self) -> Timestamp {
        self.as_ref().timestamp()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct OwnedFloEvent {
    pub id: FloEventId,
    pub timestamp: Timestamp,
    pub parent_id: Option<FloEventId>,
    pub namespace: String,
    pub data: Vec<u8>,
}

impl OwnedFloEvent {
    pub fn new(id: FloEventId, parent_id: Option<FloEventId>, timestamp: Timestamp, namespace: String, data: Vec<u8>) -> OwnedFloEvent {
        OwnedFloEvent {
            id: id,
            timestamp: timestamp,
            parent_id: parent_id,
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

    fn parent_id(&self) -> Option<FloEventId> {
        self.parent_id
    }
    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn flo_event_id_map_has_current_value_set() {
        let mut map = HashMap::new();

        map.set(FloEventId::new(2, 33));

        assert_eq!(33, map.get_counter(2));
    }

    #[test]
    fn event_is_greater_returns_true_if_event_id_is_greater_than_current() {
        let mut map = HashMap::new();

        map.set(FloEventId::new(2, 33));

        assert!(map.event_is_greater(FloEventId::new(2, 34)));
    }

    #[test]
    fn event_is_greater_returns_false_when_event_id_equals_current() {
        let mut map = HashMap::new();

        map.set(FloEventId::new(2, 33));

        assert!(!map.event_is_greater(FloEventId::new(2, 33)));
    }

    #[test]
    fn event_is_greater_returns_false_when_event_id_is_less_than_current() {
        let mut map = HashMap::new();

        map.set(FloEventId::new(2, 33));

        assert!(!map.event_is_greater(FloEventId::new(2, 32)));
    }

    #[test]
    fn event_is_greater_returns_true_if_actor_is_not_represented_in_map() {
        let mut map = HashMap::new();

        assert!(map.event_is_greater(FloEventId::new(2, 1)));
    }


}
