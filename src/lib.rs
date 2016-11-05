#![feature(conservative_impl_trait)]

mod version_map;
mod event_store;

use event_store::EventStore;
use version_map::VersionMap;
use std::collections::HashMap;

pub type ActorId = u16;
pub type EventCounter = u64;


#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct Dot {
    actor: ActorId,
    event_counter: EventCounter,
}

impl Dot {
    pub fn new(actor: ActorId, counter: EventCounter) -> Dot {
        Dot {
            actor: actor,
            event_counter: counter,
        }
    }
}

use std::cmp::Ordering;
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



#[derive(Debug, PartialEq, Clone)]
pub struct Event {
    pub id: Dot,
    pub data: Vec<u8>,
}

impl Event {
    pub fn new<T: Into<Vec<u8>>>(actor: ActorId, counter: EventCounter, bytes: T) -> Event {
        Event {
            id: Dot::new(actor, counter),
            data: bytes.into()
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Delta {
    pub actor_id: ActorId,
    pub version_map: VersionMap,
    pub events: Vec<Event>,
}

#[derive(Debug, PartialEq)]
pub struct AOSequence {
    pub actor_id: ActorId,
    pub actor_version_maps: HashMap<ActorId, VersionMap>,
    pub events: EventStore
}

impl AOSequence {

    pub fn new(actor_id: ActorId) -> AOSequence {
        AOSequence {
            actor_id: actor_id,
            actor_version_maps: HashMap::new(),
            events: EventStore::new(),
        }
    }

    pub fn push<T: Into<Vec<u8>>>(&mut self, event_data: T) -> Dot {
        let actor_id = self.actor_id;
        let new_counter = self.increment_event_counter(actor_id, actor_id);
        self.events.add_event(Event::new(actor_id, new_counter, event_data.into()));
        Dot::new(actor_id, new_counter)
    }

    pub fn join(&mut self, delta: Delta) {
        for evt in delta.events {
            self.add_event(evt);
        }
        println!("joining in versions: {:?}\nmy versions: {:?}", delta.version_map, self.actor_version_maps);
        self.update_version_vector(delta.actor_id, &delta.version_map);
        println!("finished joining in versions: {:?}\nmy versions: {:?}", delta.version_map, self.actor_version_maps);
    }

    pub fn get_delta(&mut self, other: ActorId) -> Option<Delta> {
        let events = {
            let AOSequence{ref events, ref mut actor_version_maps, ..} = *self;
            let other_version_map = actor_version_maps.entry(other).or_insert(VersionMap::new());
            events.get_delta(other, other_version_map).cloned().collect()
        };
        let my_actor_id = self.actor_id;
        let my_version_map = self.actor_version_maps.entry(my_actor_id).or_insert_with(|| VersionMap::new()).clone();
        Some(Delta{
            actor_id: my_actor_id,
            version_map: my_version_map,
            events: events,
        })
    }

    fn add_event(&mut self, event: Event) {
        let my_actor_id = self.actor_id;
        let is_new_event = {
            let map = self.actor_version_maps.entry(my_actor_id).or_insert(VersionMap::new());
            event.id.event_counter > *map.versions.get(&event.id.actor).unwrap_or(&0)
        };
        if is_new_event {
            self.increment_event_counter(my_actor_id, event.id.actor);
            self.events.add_event(event);
        }
    }

    fn update_version_vector(&mut self, actor_id: ActorId, version_vector: &VersionMap) {
        println!("Updating VersionVector from Actor: {}", actor_id);
        self.actor_version_maps.entry(actor_id).or_insert_with(|| VersionMap::new()).update(version_vector);
        println!("Finished updating VersionVector from Actor: {}", actor_id);

    }

    fn increment_event_counter(&mut self, perspective_of: ActorId, to_increment: ActorId) -> EventCounter {
        self.actor_version_maps.entry(perspective_of).or_insert_with(|| VersionMap::new()).increment(to_increment)
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

#[test]
fn two_aosequences_converge() {
    let actor_a = 0;
    let actor_b = 1;
    let mut subject_a = AOSequence::new(actor_a);
    let mut subject_b = AOSequence::new(actor_b);

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
    let mut subject_a = AOSequence::new(actor_a);
    let mut subject_b = AOSequence::new(actor_b);
    let mut subject_c = AOSequence::new(actor_c);

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
    let mut subject = AOSequence::new(actor_a);

    // Setup pre-existing events from the other actor
    let delta_to_join = {
        let mut delta_versions = VersionMap::new();
        delta_versions.increment(actor_b);
        delta_versions.increment(actor_c);
        let b_event = Event::new(actor_b, 1, "those bytes".to_owned());
        let c_event = Event::new(actor_c, 1, "c".to_owned());

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
        let mut delta_versions = VersionMap::new();
        delta_versions.increment(actor_c);
        let event = Event::new(actor_c, 2, "new C event".to_owned());
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
    assert_eq!(expected_version, result.version_map.versions);
}

#[test]
fn delta_returns_all_events_when_no_version_vector_stored_for_given_actor() {
    let mut subject = AOSequence::new(0);

    let event1 = Event::new(0, 1, "these bytes".to_owned());
    let event2 = Event::new(0, 2, "dees bytes".to_owned());
    subject.push(event1.data.clone());
    subject.push(event2.data.clone());

    // Add events that have come from a different actor
    let other_actor_id = 1;
    let other_event = Event::new(other_actor_id, 1, "those bytes".to_owned());
    let delta_to_join = {
        let mut delta_versions = VersionMap::new();
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
    assert_eq!(expected_version, result.version_map.versions);
}

#[test]
fn empty_version_vector_is_updated_to_add_all_elements_from_other() {
    let mut a = AOSequence::new(0);

    let mut other_vv = VersionMap::new();
    other_vv.increment(1);
    other_vv.increment(2);

    a.update_version_vector(1, &other_vv);

    let a_vv = a.actor_version_maps.get(&1).unwrap();
    assert_eq!(*a_vv, other_vv);
}

#[test]
fn pushing_event_returns_next_event_counter() {
    let actor_id = 5;
    let mut subject = AOSequence::new(actor_id);
    subject.push("thebytes".to_owned());
    assert_eq!(1, subject.actor_version_maps.len());
    let counter = subject.actor_version_maps.get(&actor_id).unwrap().head(actor_id);
    assert_eq!(1, counter);
}

#[test]
fn joining_empty_to_delta_adds_events() {
    let actor_id = 0;
    let mut subject = AOSequence::new(actor_id);

    let mut delta_versions = VersionMap::new();
    delta_versions.increment(5);
    delta_versions.increment(5);
    delta_versions.increment(6);

    let delta = Delta {
        actor_id: 5,
        version_map: delta_versions,
        events: vec![
            Event::new(5, 1, "event 1 bytes".to_owned()),
            Event::new(5, 2, "event 2 bytes".to_owned()),
            Event::new(6, 1, "moar bytes!!".to_owned()),
        ],
    };

    subject.join(delta);

    assert_eq!(3, subject.events.count());
}

