use ::{EventCounter, ActorId};

use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct VersionVector {
    versions: HashMap<ActorId, EventCounter>
}

impl VersionVector {
    pub fn new() -> VersionVector {
        VersionVector {
            versions: HashMap::new()
        }
    }

    pub fn update(&mut self, other: &VersionVector) {
        for (actor, counter) in other.versions.iter() {
            let existing_version = self.versions.entry(*actor).or_insert(0);
            println!("Updating entry for Actor: {} from {} to {}", actor, existing_version, counter);
            *existing_version = *counter;
        }
    }

    pub fn increment(&mut self, actor: ActorId) {
        let counter = self.versions.entry(actor).or_insert(0);
        *counter += 1;
    }

    pub fn head(&self, actor_id: ActorId) -> EventCounter {
        self.versions.get(&actor_id).map(|c| c.clone()).unwrap_or(0)
    }

}
