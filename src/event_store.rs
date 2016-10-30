use ::{ActorId, Event};
use version_vector::VersionVector;

#[derive(Debug, PartialEq)]
pub struct EventStore {
    events: Vec<Event>  //TODO: of course we'll need some
}

impl EventStore {
    pub fn new() -> EventStore {
        EventStore {
            events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: Event) {
        self.events.push(event);
    }

    //TODO: This will become trait method
    pub fn get_delta<'a>(&'a self, other_node: ActorId, version_vec: &'a VersionVector) -> impl Iterator<Item=&'a Event> + 'a {
        self.events.iter().filter(move |event| {
            let event_actor = event.id.actor;
            event_actor != other_node &&
                    event.id.event_counter > version_vec.head(event_actor)
        })
    }
}
