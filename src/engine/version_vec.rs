use std::collections::HashMap;

use flo_event::{FloEventId, ActorId, EventCounter};

#[derive(Debug, Clone, PartialEq)]
pub struct VersionVector(HashMap<ActorId, EventCounter>);

impl VersionVector {

    pub fn new() -> VersionVector {
        VersionVector(HashMap::new())
    }

    pub fn with_capacity(initial_capacity: usize) -> VersionVector {
        VersionVector(HashMap::with_capacity(initial_capacity))
    }

    pub fn from_vec(ids: Vec<FloEventId>) -> Result<VersionVector, String> {
        let mut map = HashMap::with_capacity(ids.len());
        for id in ids {
            if map.insert(id.actor, id.event_counter).is_some() {
                return Err(format!("Actor {} is represented multiple times in input", id.actor));
            }
        }
        Ok(VersionVector(map))
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

    pub fn snapshot(&self) -> Vec<FloEventId> {
        self.0.iter().map(|(actor, counter)| FloEventId::new(*actor, *counter)).collect()
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use flo_event::FloEventId;
    use std::collections::HashSet;

    #[test]
    fn from_vec_returns_error_when_there_are_multiple_entries_for_the_same_actor() {
        let input = vec![
            FloEventId::new(2, 3),
            FloEventId::new(4, 5),
            FloEventId::new(2, 2)
        ];
        assert!(VersionVector::from_vec(input).is_err());
    }

    #[test]
    fn vec_of_event_ids_is_converted_to_version_vector() {
        let input = vec![
            FloEventId::new(2, 3),
            FloEventId::new(4, 5),
            FloEventId::new(1, 2)
        ];

        let result = VersionVector::from_vec(input).unwrap();
        assert_eq!(3, result.0.len());

        assert_eq!(3, result.get(2));
        assert_eq!(5, result.get(4));
        assert_eq!(2, result.get(1));
    }

    #[test]
    fn empty_version_vector_is_converted_to_and_from_vector() {
        let start = VersionVector::new();
        let as_vec = start.snapshot();
        assert!(as_vec.is_empty());
        let end = VersionVector::from_vec(as_vec).expect("failed to create VersionVector from vec");
        assert_eq!(start, end);
    }

    #[test]
    fn snapshot_returns_vec_of_event_ids() {
        let mut subject = VersionVector::new();
        subject.update(FloEventId::new(1, 4)).expect("failed update");
        subject.update(FloEventId::new(2, 7)).expect("failed update");
        subject.update(FloEventId::new(8, 5)).expect("failed update");
        subject.update(FloEventId::new(8, 9)).expect("failed update");

        let result = subject.snapshot().into_iter().collect::<HashSet<_>>();
        let expected = vec![
            FloEventId::new(1, 4),
            FloEventId::new(2, 7),
            FloEventId::new(8, 9),
        ].into_iter().collect::<HashSet<_>>();
        assert_eq!(expected, result);
    }

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
