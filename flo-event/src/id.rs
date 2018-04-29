use std::cmp::{Ord, PartialOrd, Ordering};
use std::fmt::{self, Display};
use std::str::FromStr;


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
        write!(f, "{}.{}", self.event_counter, self.actor)
    }
}

impl FromStr for FloEventId {
    type Err = &'static str;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let err_message = "FloEventId must be an event counter and actor id separated by a single '.'";

        if let Some(dot_index) = input.find('.') {
            println!("dot index: {}", dot_index);
            let (counter, actor) = input.split_at(dot_index);
            counter.parse::<EventCounter>().and_then(|c| {
                (&actor[1..]).parse::<ActorId>().map(|a| {
                    FloEventId::new(a, c)
                })
            }).map_err(|_| err_message)
        } else {
            Err(err_message)
        }
    }
}

#[cfg(test)]
mod event_id_test {
    use super::*;

    #[test]
    fn flo_event_id_is_displayed_as_string() {
        let id = FloEventId::new(7, 12345);
        let result = format!("{}", id);
        let expected = "12345.7";
        assert_eq!(expected, &result);
    }

    #[test]
    fn from_str_returns_err_when_actor_id_is_missing() {
        assert!(FloEventId::from_str("4.").is_err())
    }

    #[test]
    fn from_str_returns_err_when_event_counter_is_missing() {
        assert!(FloEventId::from_str(".4").is_err())
    }

    #[test]
    fn from_str_returns_err_when_number_does_not_contain_dot() {
        assert!(FloEventId::from_str("7654").is_err())
    }

    #[test]
    fn flo_event_id_is_parsed_from_a_dot_separated_string() {
        let input = "8.2";
        let result = FloEventId::from_str(input).unwrap();
        assert_eq!(FloEventId::new(2, 8), result);
    }
}

pub const ZERO_EVENT_ID: FloEventId = FloEventId{event_counter: 0, actor: 0};
pub const MAX_EVENT_ID: FloEventId = FloEventId{event_counter: ::std::u64::MAX, actor: ::std::u16::MAX};

impl FloEventId {

    /// A zero id represents the lack of an id. All valid ids start with an event counter of 1.
    #[inline]
    pub fn zero() -> FloEventId {
        ZERO_EVENT_ID
    }

    /// Returns the maximum possible event id. All other event ids will be less than or equal to this id
    #[inline]
    pub fn max() -> FloEventId {
        MAX_EVENT_ID
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
