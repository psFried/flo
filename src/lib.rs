#![feature(conservative_impl_trait)]

mod version_vector;
mod event_store;

use event_store::EventStore;
use version_vector::VersionVector;
use std::collections::HashMap;

pub type ActorId = u16;
pub type EventCounter = u64;


#[derive(Debug, PartialEq)]
pub struct EventId {
    actor: ActorId,
    event_counter: EventCounter,
}

impl EventId {
    pub fn new(actor: ActorId, counter: EventCounter) -> EventId {
        EventId {
            actor: actor,
            event_counter: counter,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Event {
    id: EventId,
    data: Vec<u8>,
}

impl Event {
    pub fn new<T: Into<Vec<u8>>>(actor: ActorId, counter: EventCounter, bytes: T) -> Event {
        Event {
            id: EventId::new(actor, counter),
            data: bytes.into()
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct EventList {
    pub actor_id: ActorId,
    pub version_vectors: HashMap<ActorId, VersionVector>,
    pub events: EventStore
}

impl EventList {

    pub fn new(actor_id: ActorId) -> EventList {
        EventList {
            actor_id: actor_id,
            version_vectors: HashMap::new(),
            events: EventStore::new(),
        }
    }

    pub fn add_event(&mut self, event: Event) {
        self.events.add_event(event);
    }

    pub fn update_version_vector(&mut self, actor_id: ActorId, version_vector: &VersionVector) {
        println!("Updating VersionVector from Actor: {}", actor_id);
        self.version_vectors.entry(actor_id).or_insert_with(|| VersionVector::new()).update(version_vector);
        println!("Finished updating VersionVector from Actor: {}", actor_id);

    }

//    pub fn get_delta(&mut self)
}



/*
# Version Vectors

- Can maybe be represented as just a pointer to the HEAD counter for each actor???

# Delta States

- Each node keeps the version vector of every other node
- A node always sends the current HEAD for each node that it knows of
- Computing the Delta State for a given node `X`:
    - For each node `N` in the version vector, excluding node `X` itself:
        - Include all events with counter > the highest known counter in the version vector for `N`

# Joining to Delta States

- Just add all new events to the list of events!!!
- Send out complete state of the updated version vector

*/

#[cfg(test)]
mod tests {
    use super::*;
    use version_vector::VersionVector;
    use event_store::EventStore;

    #[test]
    fn it_works() {
        let mut a = EventList::new(0);

        let mut other_vv = VersionVector::new();
        other_vv.increment(1);
        other_vv.increment(2);

        a.update_version_vector(1, &other_vv);

        let a_vv = a.version_vectors.get(&1).unwrap();
        assert_eq!(*a_vv, other_vv);
    }
}
