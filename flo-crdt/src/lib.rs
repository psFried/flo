#![feature(conservative_impl_trait)]

#[macro_use]
extern crate log;

#[cfg(test)]
#[macro_use]
extern crate test_logger;

#[cfg(test)]
extern crate env_logger;

mod version_map;
mod element_store;
mod dot;

use element_store::ElementStore;
use version_map::{VersionMap, ActorVersionMaps};
use std::collections::HashMap;
use std::marker::PhantomData;

pub use dot::{ActorId, ElementCounter, Dot};


#[derive(Debug, PartialEq, Clone)]
pub struct Element {
    pub id: Dot,
    pub data: Vec<u8>,
}

impl Element {
    pub fn new<T: Into<Vec<u8>>>(actor: ActorId, counter: ElementCounter, bytes: T) -> Element {
        Element {
            id: Dot::new(actor, counter),
            data: bytes.into()
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Delta<T: VersionMap> {
    pub actor_id: ActorId,
    pub version_map: T,
    pub events: Vec<Element>,
}

#[derive(Debug, PartialEq)]
pub struct AOSequence<T: VersionMap, A: ActorVersionMaps<T>> {
    pub actor_id: ActorId,
    pub actor_version_maps: A,
    pub events: ElementStore,
    phantom_data: PhantomData<T>,
}

pub type InMemoryAOSequence = AOSequence<HashMap<ActorId, ElementCounter>, HashMap<ActorId, HashMap<ActorId, ElementCounter>>>;

impl <T: VersionMap, A: ActorVersionMaps<T>> AOSequence<T, A> {

    pub fn new_in_memory(actor_id: ActorId) -> InMemoryAOSequence {
        AOSequence::new(actor_id, HashMap::new())
    }

    pub fn new(actor_id: ActorId, actor_version_maps: A) -> AOSequence<T, A> {
        AOSequence {
            actor_id: actor_id,
            actor_version_maps: actor_version_maps,
            events: ElementStore::new(),
            phantom_data: PhantomData,
        }
    }

    pub fn push<B: Into<Vec<u8>>>(&mut self, event_data: B) -> Dot {
        let actor_id = self.actor_id;
        let new_counter = self.increment_event_counter(actor_id, actor_id);
        self.events.add_element(Element::new(actor_id, new_counter, event_data.into()));
        Dot::new(actor_id, new_counter)
    }

    pub fn join(&mut self, delta: Delta<T>) {
        for evt in delta.events {
            self.add_element(evt);
        }
        debug!("joining in versions: {:?}\nmy versions: {:?}", delta.version_map, self.actor_version_maps);
        self.update_version_vector(delta.actor_id, &delta.version_map);
        debug!("finished joining in versions: {:?}\nmy versions: {:?}", delta.version_map, self.actor_version_maps);
    }

    pub fn get_delta(&mut self, other: ActorId) -> Option<Delta<T>> {
        let events = {
            let AOSequence{ref events, ref mut actor_version_maps, ..} = *self;
            let other_version_map = actor_version_maps.get_version_map(other);
            events.get_delta(other, other_version_map).cloned().collect()
        };
        let my_actor_id = self.actor_id;
        let my_version_map = self.actor_version_maps.get_version_map(my_actor_id).clone();
        Some(Delta{
            actor_id: my_actor_id,
            version_map: my_version_map,
            events: events,
        })
    }

    fn add_element(&mut self, event: Element) {
        let my_actor_id = self.actor_id;
        let is_new_event = self.actor_version_maps.get_element_counter(my_actor_id, event.id.actor) < event.id.counter;
        if is_new_event {
            self.increment_event_counter(my_actor_id, event.id.actor);
            self.events.add_element(event);
        }
    }

    fn update_version_vector(&mut self, actor_id: ActorId, version_vector: &T) {
        debug!("Updating VersionVector from Actor: {}", actor_id);
        self.actor_version_maps.update(actor_id, version_vector);
        debug!("Finished updating VersionVector from Actor: {}", actor_id);

    }

    fn increment_event_counter(&mut self, perspective_of: ActorId, to_increment: ActorId) -> ElementCounter {
        self.actor_version_maps.increment_element_counter(perspective_of, to_increment)
    }
}

/*
# Version Vectors

- Can maybe be represented as just a pointer to the HEAD counter for each actor
    - This works because we will never update events out of sequence because a delta state always begins at the last acknowledged event

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
mod test {
    use super::*;
    use element_store::ElementStore;
    use version_map::{VersionMap, ActorVersionMaps};
    use std::collections::HashMap;
    use std;
    use test_logger;

    fn new_in_memory(actor_id: ActorId) -> InMemoryAOSequence {
        AOSequence::new(actor_id, HashMap::new())
    }

    #[test]
    fn two_aosequences_converge() {
        let actor_a: ActorId = 0;
        let actor_b = 1;
        let mut subject_a = new_in_memory(actor_a);
        let mut subject_b = new_in_memory(actor_b);

        subject_a.push("A event 1".to_owned());

        subject_b.join(subject_a.get_delta(actor_b).unwrap());

        subject_a.push("A event 2".to_owned());

        subject_b.push("B event 1".to_owned());
        subject_b.get_delta(actor_a); // This delta does not get joined to A

        subject_b.push("B event 2".to_owned());
        subject_a.join(subject_b.get_delta(actor_a).unwrap());
        subject_b.join(subject_a.get_delta(actor_b).unwrap());

        let expected = vec!["A event 1", "B event 1", "A event 2", "B event 2"];
        let a_events = subject_a.events.iter_range(Dot::new(0, 0))
                .map(|e| std::str::from_utf8(&e.data).unwrap())
                .collect::<Vec<&str>>();
        assert_eq!(expected, a_events);
        let b_events = subject_b.events.iter_range(Dot::new(0, 0))
                                       .map(|e| std::str::from_utf8(&e.data).unwrap())
                                       .collect::<Vec<&str>>();
        assert_eq!(expected, b_events);
    }

    #[test]
    fn three_aosequences_converge() {
        let actor_a = 0;
        let actor_b = 1;
        let actor_c = 2;
        let mut subject_a = new_in_memory(actor_a);
        let mut subject_b = new_in_memory(actor_b);
        let mut subject_c = new_in_memory(actor_c);

        subject_a.push("A1".to_owned());

        subject_b.join(subject_a.get_delta(actor_b).unwrap());

        subject_a.push("A2".to_owned());
        subject_b.push("B1".to_owned());
        subject_c.push("C1".to_owned());

        subject_c.join(subject_b.get_delta(actor_c).unwrap());

        subject_a.join(subject_c.get_delta(actor_a).unwrap());
        subject_b.join(subject_a.get_delta(actor_b).unwrap());

        let a_events = subject_a.events.iter_range(Dot::new(0, 0))
                                       .map(|e| std::str::from_utf8(&e.data).unwrap())
                                       .collect::<Vec<&str>>();
        assert_eq!(vec!["A1", "B1", "C1", "A2"], a_events);
        let b_events = subject_b.events.iter_range(Dot::new(0, 0))
                                       .map(|e| std::str::from_utf8(&e.data).unwrap())
                                       .collect::<Vec<&str>>();
        assert_eq!(vec!["A1", "B1", "C1", "A2"], b_events);

        let c_events = subject_c.events.iter_range(Dot::new(0, 0))
                                       .map(|e| std::str::from_utf8(&e.data).unwrap())
                                       .collect::<Vec<&str>>();
        assert_eq!(vec!["A1", "B1", "C1"], c_events);
    }

    #[test]
    fn delta_excludes_events_that_the_other_actor_already_has() {
        let actor_a = 0;
        let actor_b = 1;
        let actor_c = 2;
        let mut subject = new_in_memory(actor_a);

        // Setup pre-existing events from the other actor
        let delta_to_join = {
            let mut delta_versions = HashMap::new();
            delta_versions.increment(actor_b);
            delta_versions.increment(actor_c);
            let b_event = Element::new(actor_b, 1, "those bytes".to_owned());
            let c_event = Element::new(actor_c, 1, "c".to_owned());

            Delta {
                actor_id: actor_b,
                version_map: delta_versions,
                events: vec![b_event, c_event]
            }
        };
        subject.join(delta_to_join);

        // add new events
        subject.push("new A event".to_owned());
        let delta_to_join = {
            let mut delta_versions = HashMap::new();
            delta_versions.increment(actor_c);
            let event = Element::new(actor_c, 2, "new C event".to_owned());
            Delta {
                actor_id: actor_c,
                version_map: delta_versions,
                events: vec![event]
            }
        };
        subject.join(delta_to_join);


        let result = subject.get_delta(actor_b).unwrap();
        assert_eq!(actor_a, result.actor_id);

        let event_data: Vec<&str> = result.events.iter().map(|evt| std::str::from_utf8(&evt.data).unwrap()).collect();
        assert_eq!(vec!["new A event", "new C event"], event_data);

        let mut expected_version = HashMap::new();
        expected_version.insert(actor_a, 1);
        expected_version.insert(actor_b, 1);
        expected_version.insert(actor_c, 2);
        assert_eq!(Some(1), result.version_map.get_element_counter(actor_a));
        assert_eq!(Some(1), result.version_map.get_element_counter(actor_b));
        assert_eq!(Some(2), result.version_map.get_element_counter(actor_c));
    }

    #[test]
    fn delta_returns_all_events_when_no_version_vector_stored_for_given_actor() {
        let mut subject = new_in_memory(0);

        let event1 = Element::new(0, 1, "these bytes".to_owned());
        let event2 = Element::new(0, 2, "dees bytes".to_owned());
        subject.push(event1.data.clone());
        subject.push(event2.data.clone());

        // Add events that have come from a different actor
        let other_actor_id = 1;
        let other_event = Element::new(other_actor_id, 1, "those bytes".to_owned());
        let delta_to_join = {
            let mut delta_versions = HashMap::new();
            delta_versions.increment(other_actor_id);
            Delta {
                actor_id: other_actor_id,
                version_map: delta_versions,
                events: vec![other_event.clone()]
            }
        };
        subject.join(delta_to_join);

        let result = subject.get_delta(2).unwrap();
        assert_eq!(0, result.actor_id);
        assert_eq!(vec![event1, other_event, event2], result.events);

        let mut expected_version = HashMap::new();
        expected_version.insert(0, 2);
        expected_version.insert(1, 1);
        assert_eq!(Some(2), result.version_map.get_element_counter(0u16));
        assert_eq!(Some(1), result.version_map.get_element_counter(1u16));
    }

    #[test]
    fn empty_version_vector_is_updated_to_add_all_elements_from_other() {
        let mut a = new_in_memory(0);

        let mut other_vv = HashMap::new();
        other_vv.increment(1);
        other_vv.increment(2);

        a.update_version_vector(1, &other_vv);

        let a_vv = a.actor_version_maps.get(&1).unwrap();
        assert_eq!(*a_vv, other_vv);
    }

    #[test]
    fn pushing_event_returns_next_event_counter() {
        let actor_id = 5;
        let mut subject = new_in_memory(actor_id);
        subject.push("thebytes".to_owned());
        assert_eq!(1, subject.actor_version_maps.len());
        let counter = subject.actor_version_maps.get_element_counter(actor_id, actor_id);
        assert_eq!(1, counter);
    }

    #[test]
    fn joining_empty_to_delta_adds_events() {
        let actor_id = 0;
        let mut subject = new_in_memory(actor_id);

        let mut delta_versions = HashMap::new();
        delta_versions.increment(5);
        delta_versions.increment(5);
        delta_versions.increment(6);

        let delta = Delta {
            actor_id: 5,
            version_map: delta_versions,
            events: vec![
                Element::new(5, 1, "event 1 bytes".to_owned()),
                Element::new(5, 2, "event 2 bytes".to_owned()),
                Element::new(6, 1, "moar bytes!!".to_owned()),
            ],
        };

        subject.join(delta);

        assert_eq!(3, subject.events.count());
    }

}
