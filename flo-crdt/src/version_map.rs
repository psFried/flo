use ::{ActorId, ElementCounter};
use std::collections::HashMap;

use std::fmt::Debug;

pub trait ActorVersionMaps<M: VersionMap>: PartialEq + Clone + Debug + Sized {
    /// Should create a new map if one does not exist
    fn get_version_map(&mut self, actor: ActorId) -> &M;

    fn get_element_counter(&self, perspective: ActorId, actor: ActorId) -> ElementCounter;

    fn increment_element_counter(&mut self, perspective: ActorId, actor: ActorId) -> ElementCounter;

    fn update(&mut self, perspective: ActorId, other_version_map: &M);
}

impl <M: VersionMap> ActorVersionMaps<M> for HashMap<ActorId, M> {
    fn get_version_map(&mut self, actor: ActorId) -> &M {
        self.entry(actor).or_insert_with(|| M::new())
    }

    fn get_element_counter(&self, perspective: ActorId, actor: ActorId) -> ElementCounter {
        self.get(&perspective).and_then(|version_map| {
            version_map.get_element_counter(actor)
        }).unwrap_or(0)
    }

    fn increment_element_counter(&mut self, perspective: ActorId, actor: ActorId) -> ElementCounter {
        self.entry(perspective).or_insert_with(|| M::new()).increment(actor)
    }

    fn update(&mut self, perspective: ActorId, other_version_map: &M) {
        self.entry(perspective).or_insert_with(|| M::new()).update(other_version_map);
    }
}

pub trait VersionMap: PartialEq + Clone + Debug + Sized {
    fn new() -> Self;

    fn get_element_counter(&self, actor_id: ActorId) -> Option<ElementCounter>;

    fn update(&mut self, other: &Self);

    fn increment(&mut self, actor: ActorId) -> ElementCounter;
}

impl VersionMap for HashMap<ActorId, ElementCounter> {

    fn new() -> Self {
        HashMap::with_capacity(8)
    }

    fn get_element_counter(&self, actor_id: ActorId) -> Option<ElementCounter> {
        self.get(&actor_id).map(|c| *c)
    }

    fn update(&mut self, other: &Self) {
        for (actor, counter) in other.iter() {
            let existing_version = self.entry(*actor).or_insert(0);
            debug!("Updating entry for Actor: {} from {} to {}", actor, existing_version, counter);
            *existing_version = *counter;
        }
    }

    fn increment(&mut self, actor: ActorId) -> ElementCounter {
        let counter = self.entry(actor).or_insert(0);
        *counter += 1;
        *counter
    }
}
