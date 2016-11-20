
use std::cmp::Ordering;

pub type ActorId = u16;
pub type ElementCounter = u64;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct Dot {
    pub actor: ActorId,
    pub counter: ElementCounter,
}

impl Dot {
    pub fn new(actor: ActorId, counter: ElementCounter) -> Dot {
        Dot {
            actor: actor,
            counter: counter,
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
        if self.counter > other.counter {
            Ordering::Greater
        } else if self.counter < other.counter {
            Ordering::Less
        } else {
            self.actor.cmp(&other.actor)
        }
    }
}
