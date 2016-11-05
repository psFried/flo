
use std::cmp::Ordering;

pub type ActorId = u16;
pub type EventCounter = u64;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct Dot {
    pub actor: ActorId,
    pub event_counter: EventCounter,
}

impl Dot {
    pub fn new(actor: ActorId, counter: EventCounter) -> Dot {
        Dot {
            actor: actor,
            event_counter: counter,
        }
    }
}

impl PartialOrd for Dot {
    fn partial_cmp(&self, other: &Dot) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Dot {

    fn cmp(&self, other: &Dot) -> Ordering {
        if self.event_counter > other.event_counter {
            Ordering::Greater
        } else if self.event_counter < other.event_counter {
            Ordering::Less
        } else {
            self.actor.cmp(&other.actor)
        }
    }
}
