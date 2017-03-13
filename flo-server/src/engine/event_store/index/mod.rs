use event::{FloEventId, ActorId, VersionVector};

use std::collections::{BTreeMap, Bound};

#[derive(PartialEq, Debug, Clone)]
pub struct IndexEntry {
    pub id: FloEventId,
    pub offset: u64,
}

impl IndexEntry {
    pub fn new(id: FloEventId, offset: u64) -> IndexEntry {
        IndexEntry {
            id: id,
            offset: offset,
        }
    }
}

pub struct EventIndex {
    entries: BTreeMap<FloEventId, IndexEntry>,
    max_entries: usize,
    least_entry: FloEventId,
    greatest_entry: FloEventId,
    version_vec: VersionVector,
}

impl EventIndex {
    pub fn new(max_events: usize) -> EventIndex {
        EventIndex {
            entries: BTreeMap::new(),
            max_entries: max_events,
            least_entry: FloEventId::new(0, 0),
            greatest_entry: FloEventId::new(0, 0),
            version_vec: VersionVector::new(),
        }
    }

    pub fn add(&mut self, new_entry: IndexEntry) -> Option<IndexEntry> {
        self.version_vec.update_if_greater(new_entry.id);

        let mut to_return = None;
        trace!("adding index entry: {:?}", new_entry);

        if self.least_entry.is_zero() {
            self.least_entry = new_entry.id;
        }

        if new_entry.id > self.greatest_entry {
            self.greatest_entry = new_entry.id;
        }

        if self.entries.len() == self.max_entries - 1 {
            let to_remove = self.least_entry;
            to_return = self.entries.remove(&to_remove);
            let new_min = self.entries.keys().next().expect("Must have at least one entry in index since it is over capacity");
            self.least_entry = *new_min;
        }
        self.entries.insert(new_entry.id, new_entry);
        to_return
    }

    pub fn get_version_vector(&self) -> &VersionVector {
        &self.version_vec
    }

    pub fn get_next_entry(&self, start_after: FloEventId) -> Option<&IndexEntry> {
        self.entries.range((Bound::Excluded(&start_after), Bound::Unbounded)).next().map(|(_k, v)| v)
    }

    pub fn get_next_entry_for_actor(&self, start_after: FloEventId, actor_id: ActorId) -> Option<&IndexEntry> {
        self.entries.range((Bound::Excluded(&start_after), Bound::Unbounded))
                .filter(|&(id, _entry)| {
                    id.actor == actor_id
                }).next()
                .map(|(_id, entry)| entry)
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }
}


#[cfg(test)]
mod index_test {
    use super::*;
    use event::{FloEventId, ActorId, EventCounter};

    const ACTOR_ID: ActorId = 1;

    fn id_entry(actor: ActorId, counter: EventCounter) -> IndexEntry {
        IndexEntry::new(FloEventId::new(actor, counter), 76)
    }

    #[test]
    fn version_vector_is_kept_up_to_date() {
        let mut subject = EventIndex::new(10);
        subject.add(id_entry(2, 3));
        subject.add(id_entry(4, 2));
        subject.add(id_entry(5, 23));

        let version_vec = subject.get_version_vector();
        assert_eq!(3, version_vec.get(2));
        assert_eq!(2, version_vec.get(4));
        assert_eq!(23, version_vec.get(5));
    }

    #[test]
    fn get_next_entry_returns_first_entry_when_start_id_is_zero() {
        let mut subject = EventIndex::new(10);
        let entry = entry(5, 9);

        subject.add(entry.clone());
        let result = subject.get_next_entry(FloEventId::new(0, 0));
        assert_eq!(Some(&entry), result);
    }

    #[test]
    fn adding_an_entry_returns_removed_entry_when_total_entries_exceeds_max() {
        let mut subject = EventIndex::new(10);

        for i in 1..10 {
            let result = subject.add(entry(i, i));
            assert!(result.is_none());
        }

        let result = subject.add(entry(11, 11));
        assert_eq!(Some(entry(1, 1)), result);
        let result = subject.add(entry(12, 12));
        assert_eq!(Some(entry(2, 2)), result);
    }

    fn entry(counter: EventCounter, offset: u64) -> IndexEntry {
        IndexEntry::new(id(counter), offset)
    }

    fn id(counter: EventCounter) -> FloEventId {
        FloEventId::new(ACTOR_ID, counter)
    }
}


