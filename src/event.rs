extern crate chrono;

use std::cmp::{Ord, PartialOrd, Ordering};
use std::fmt::{self, Display, Debug};
use std::str::FromStr;

use chrono::{DateTime, UTC};

pub type Timestamp = DateTime<UTC>;

pub type ActorId = u16;
pub type EventCounter = u64;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct FloEventId {
    pub actor: ActorId,
    pub event_counter: EventCounter,
}

impl Display for FloEventId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:020}{:05}", self.event_counter, self.actor)
    }
}

impl FromStr for FloEventId {
    type Err = &'static str;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let err_message = "FloEventId must be a 25 character string with all numeric digits";
        let counter_portion = input[..20].parse::<EventCounter>().map_err(|_| err_message)?;
        let actor_portion = input[20..].parse::<ActorId>().map_err(|_| err_message)?;
        Ok(FloEventId::new(actor_portion, counter_portion))
    }
}

#[cfg(test)]
mod event_id_test {
    use super::*;

    #[test]
    fn flo_event_id_is_displayed_as_string() {
        let id = FloEventId::new(7, 12345);
        let result = format!("{}", id);
        let expected = "0000000000000001234500007";
        assert_eq!(expected, &result);
    }

    #[test]
    fn flo_event_id_is_parsed_from_25_digit_string() {
        let start = FloEventId::new(12345, 877655);
        let as_str = format!("{}", start);
        let result = as_str.parse::<FloEventId>().expect("failed to parse id");
        assert_eq!(start, result);
    }
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
