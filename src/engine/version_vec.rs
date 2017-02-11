use std::collections::HashMap;

use flo_event::{FloEventId, ActorId, EventCounter};

pub struct VersionVector(HashMap<ActorId, EventCounter>);

impl VersionVector {

    pub fn new() -> VersionVector {
        VersionVector(HashMap::new())
    }

    pub fn with_capacity(initial_capacity: usize) -> VersionVector {
        VersionVector(HashMap::with_capacity(initial_capacity))
    }

    pub fn update(&mut self, id: FloEventId) -> Result<(), String> {
        let current = *self.0.entry(id.actor).or_insert(0);
        if id.event_counter <= current {
            Err(format!("Cannot insert event id: {:?} because the current counter: {} is greater", id, current))
        } else {
            self.0.insert(id.actor, id.event_counter);
            Ok(())
        }
    }

    pub fn get(&self, actor: ActorId) -> EventCounter {
        self.0.get(&actor).map(|c| *c).unwrap_or(0)
    }

}


#[cfg(test)]
mod test {
    use super::*;
    use flo_event::FloEventId;

    #[test]
    fn update_returns_error_when_new_event_counter_is_less_than_existing_counter() {
        let mut subject = VersionVector::new();
        subject.update(FloEventId::new(4, 4)).expect("failed first update");

        let result = subject.update(FloEventId::new(4, 3));
        assert!(result.is_err());

        let result = subject.update(FloEventId::new(4, 4));
        assert!(result.is_err());
    }

    #[test]
    fn update_sets_event_counter_for_actor_when_actor_did_not_already_exist() {
        let mut subject = VersionVector::new();
        subject.update(FloEventId::new(7, 9)).expect("failed to update version vector");
        let result = subject.get(7);
        assert_eq!(9, result);

        subject.update(FloEventId::new(7, 11)).expect("failed to update to 7-11");
        assert_eq!(11, subject.get(7));
    }

    #[test]
    fn empty_version_vector_returns_0_when_get_is_called_for_any_actor_id() {
        let subject = VersionVector::new();

        for i in 0..99 {
            let result = subject.get(i);
            assert_eq!(0, result);
        }
    }
}
