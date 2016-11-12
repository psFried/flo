use ::{ActorId, Event, Dot};
use version_map::VersionMap;

#[derive(Debug, PartialEq)]
pub struct EventStore { //TODO: change EventStore to a trait
    events: Vec<Event>
}

impl EventStore {
    pub fn new() -> EventStore {
        EventStore {
            events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: Event) {
        self.events.push(event);
        self.events.sort_by_key(|e| e.id);
    }

    pub fn count(&self) -> usize {
        self.events.len()
    }

    pub fn iter_range<'a>(&'a self, start: Dot) -> impl Iterator<Item=&'a Event> {
        self.events.iter().filter(move |event| {
            event.id > start
        })
    }

    pub fn get_delta<'a>(&'a self, other_node: ActorId, version_vec: &'a VersionMap) -> impl Iterator<Item=&'a Event> + 'a {
        self.events.iter().filter(move |event| {
            let event_actor = event.id.actor;
            event_actor != other_node && event.id.event_counter > version_vec.head(event_actor)
        })
    }
}

