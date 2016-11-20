use ::{ActorId, ElementCounter};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone)]
pub struct VersionMap {
    versions: HashMap<ActorId, ElementCounter>
}

impl VersionMap {
    pub fn new() -> VersionMap {
        VersionMap {
            versions: HashMap::new()
        }
    }

    pub fn get_element_counter(&self, actor_id: ActorId) -> Option<ElementCounter> {
        self.versions.get(&actor_id).map(|c| *c)
    }

    pub fn update(&mut self, other: &VersionMap) {
        for (actor, counter) in other.versions.iter() {
            let existing_version = self.versions.entry(*actor).or_insert(0);
            debug!("Updating entry for Actor: {} from {} to {}", actor, existing_version, counter);
            *existing_version = *counter;
        }
    }

    pub fn increment(&mut self, actor: ActorId) -> ElementCounter {
        let counter = self.versions.entry(actor).or_insert(0);
        *counter += 1;
        *counter
    }

    pub fn head(&self, actor_id: ActorId) -> ElementCounter {
        self.versions.get(&actor_id).map(|c| *c).unwrap_or(0)
    }

}
