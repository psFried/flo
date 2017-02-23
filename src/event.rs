//! This module defines the common types that form the basic building blocks of flo, events.
//! An Event is exactly what it sounds like. Semantically, it describes something that _happened_. Events are fundamentally
//! different than typical _domain objects_, which are focused on _state_.
//!
//! Events are also persistent. Many consumers can _consume_ the same event, and a single consumer can even go back and
//! read the same event multiple times if it chooses to. This makes an event stream much more efficient that queue-based
//! topics in situations with many consumers.
//!
//! Flo allows each event to declare a namespace. By convention, the namespaces of events describe the hierarchy within the
//! domain. Consider the example of modeling the operations in a restaurant. The namespaces in your domain might look
//! something like:
//!
//! <pre>
//! restaurant/
//!    ├── dining-room
//!    │   ├── bar
//!    │   └── tables
//!    │       ├── 1
//!    │       ├── 2
//!    │       └── 3
//!    ├── kitchen
//!    │   ├── dishwasher
//!    │   ├── fridge
//!    │   └── oven
//!    └── staff
//!        ├── Jen
//!        └── Robbie
//! </pre>
//! So, an event pertaining to the dishwasher would be in the namespace `/restaurant/kitchen/dishwasher`, while an event
//! related just to table 1 would be in `/restaurant/dining-room/tables/1`. If a consumer was interested in events
//! related to _all tables_, then they would read events from `/restaurant/dining-room/tables/*`, or if they were interested
//! in all events in the dining room, whether at a table or at the bar, they could consume events from
//! `/restaurant/dining-room/**/*`. This allows application services to be appropriately scoped, while still giving then
//! the freedom to cross-cut concerns. For instance, we might want to deploy a monitoring service to collect performance
//! statistics on the entire system. That monitoring service would surely want to get every event, so could read events
//! from `/**/*`.
//!
extern crate chrono;

use std::cmp::{Ord, PartialOrd, Ordering};
use std::fmt::{self, Display, Debug};
use std::str::FromStr;

use chrono::{DateTime, UTC};

/// All event timestamps are non-monotonic UTC timestamps with millisecond precision. Although the chrono crate can represent
/// nanosecond precision, this resolution is not preserved by flo's wire protocol.
pub type Timestamp = DateTime<UTC>;

/// An actor is a flo server instance that participates in the cluster. Each actor must have it's own id that is unique
/// among all the running flo server instances
pub type ActorId = u16;

/// This is just a dumb counter that is increased sequentially for each event produced. Note that each actor keeps it's own
/// EventCounter, but will fast-forward the counter to always be one greater than the highest known event counter at any
/// given point in time.
pub type EventCounter = u64;

/// This is the primary key for an event. It is immutable and unique within the entire event stream.
/// FloEventIds are strictly ordered first based on the `EventCounter` and then on the `ActorId`.
/// This ordering is exactly the same as the ordering of events in the stream.
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

    /// A zero id represents the lack of an id. All valid ids start with an event counter of 1.
    #[inline]
    pub fn zero() -> FloEventId {
        ZERO_EVENT_ID
    }

    /// Constructs a new FloEventId with the given actor and counter values
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

/// Defines an event from a client's perspective. An event Consists of some specific header information followed by
/// an optional section of binary data. Flo imposes no restrictions or opinions on what can be contained in the `data`
/// section.
pub trait FloEvent: Debug {
    /// The primary key of the event. This is immutable and never changes.
    fn id(&self) -> &FloEventId;
    /// The UTC timestamp (generated by the server) when this event was persisted.
    fn timestamp(&self) -> Timestamp;
    /// Events may optionally have a parent id. This is used to correlate events. A simple example would be a request/response
    /// where the response has it's `parent_id` set to the `id` of the request. It can also be used to trace events through a
    /// complex system of microservices. Clients are encouraged to keep it simple and just always set the `parent_id` to
    /// the `id` of whichever event is being processed at the time.
    fn parent_id(&self) -> Option<FloEventId>;
    /// Every event is produced to a namespace. Technically, this value can be any valid utf-8 string (except for newline `\n`
    /// characters). By convention, the namespace is a hierarchical path separated by forward slash `/` characters. This
    /// allows consumers to read events from multiple namespaces simultaneously by using glob syntax.
    fn namespace(&self) -> &str;
    /// Returns the total length of the data associated with this event.
    fn data_len(&self) -> u32;
    /// Returns the arbitrary binary data associated with this event.
    fn data(&self) -> &[u8];
    /// Converts this event into an `OwnedFloEvent`, cloning it in the process.
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

/// This is the main `FloEvent` implementation that clients will deal with. All of the fields returned by the `FloEvent`
/// methods are simply owned fields in this struct.
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
