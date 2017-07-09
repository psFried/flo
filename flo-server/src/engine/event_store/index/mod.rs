use event::{FloEventId, ActorId, EventCounter, VersionVector};

use std::collections::{BTreeMap, Bound};

#[derive(PartialEq, Debug, Clone)]
pub struct IndexEntry {
    pub id: FloEventId,
    pub offset: u64,
    pub segment: u64,
}

impl IndexEntry {
    pub fn new(id: FloEventId, offset: u64, segment: u64) -> IndexEntry {
        IndexEntry {
            id: id,
            offset: offset,
            segment: segment
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
struct InternalEntry {
    offset: u64,
    segment: u64,
}


pub struct EventIndex {
    entries: BTreeMap<ActorId, BTreeMap<EventCounter, InternalEntry>>,
    version_vec: VersionVector,
}

impl EventIndex {
    pub fn new() -> EventIndex {
        EventIndex {
            entries: BTreeMap::new(),
            version_vec: VersionVector::new(),
        }
    }

    pub fn add(&mut self, new_entry: IndexEntry) {
        self.version_vec.update_if_greater(new_entry.id);

        trace!("adding index entry: {:?}", new_entry);
        let internal = InternalEntry {
            offset: new_entry.offset,
            segment: new_entry.segment,
        };
        self.entries.entry(new_entry.id.actor).or_insert_with(|| BTreeMap::new()).insert(new_entry.id.event_counter, internal);
    }

    pub fn get_consumer_start_point(&self, consumer_vector: &VersionVector) -> ConsumerStartIter {

        let mut complete_vector: Vec<FloEventId> = self.version_vec.iter().map(|index_id| {
            let actor = index_id.actor;
            let counter = consumer_vector.get(actor);
            FloEventId::new(actor, counter)
        }).collect();
        complete_vector.sort();

        ConsumerStartIter {
            index: self,
            consumer_vector: complete_vector
        }
    }

    pub fn remove_range_inclusive(&mut self, end_inclusive: &VersionVector) {
        for id in end_inclusive.iter() {
            if let Some(actor_entries) = self.entries.get_mut(&id.actor) {
                let counter_end_range = id.event_counter + 1;
                let mut new_entries = actor_entries.split_off(&counter_end_range);
                ::std::mem::swap(&mut new_entries, actor_entries);
                let old_entries = new_entries;
                debug!("Removed {} entries for actor_id: {}, end_range: {}, new_start_range: {:?}",
                old_entries.len(),
                id.actor,
                counter_end_range,
                actor_entries.iter().next());
            }
        }
    }

    pub fn get_version_vector(&self) -> &VersionVector {
        &self.version_vec
    }

    fn get_next_entry_for_actor(&self, start_after: FloEventId) -> Option<ConsumerEntries> {
        self.entries.get(&start_after.actor).and_then(|actor_entries| {
            get_entry_for_actor(start_after, actor_entries)
        })
    }
}

fn get_entry_for_actor(start: FloEventId, actor_entries: &BTreeMap<EventCounter, InternalEntry>) -> Option<ConsumerEntries> {
    let mut range = actor_entries.range((Bound::Excluded(start.event_counter), Bound::Unbounded));
    let actor = start.actor;

    range.next().map(|(counter, internal)| {
        IndexEntry {
            id: FloEventId::new(actor, *counter),
            offset: internal.offset,
            segment: internal.segment,
        }
    }).map(|start_entry| {
        match range.next_back() {
            Some((counter, internal)) => {
                let end = IndexEntry {
                    id: FloEventId::new(actor, *counter),
                    offset: internal.offset,
                    segment: internal.segment,
                };
                ConsumerEntries {
                    start: start_entry,
                    end: end
                }
            }
            None => {
                let end = start_entry.clone();
                ConsumerEntries {
                    start: start_entry,
                    end: end,
                }
            }
        }
    })
}

#[derive(Debug, PartialEq)]
pub struct ConsumerEntries {
    pub start: IndexEntry,
    pub end: IndexEntry,
}

pub struct ConsumerStartIter<'a> {
    index: &'a EventIndex,
    consumer_vector: Vec<FloEventId>,
}

impl <'a> Iterator for ConsumerStartIter<'a> {
    type Item = ConsumerEntries;

    fn next(&mut self) -> Option<Self::Item> {
        let ConsumerStartIter {ref mut index, ref mut consumer_vector} = *self;

        while let Some(id) = consumer_vector.pop() {
            let result = index.get_next_entry_for_actor(id);
            if result.is_some() {
                return result;
            }
        }
        None
    }
}


#[cfg(test)]
mod index_test {
    use super::*;
    use event::{FloEventId, ActorId, EventCounter};

    fn id_entry(actor: ActorId, counter: EventCounter) -> IndexEntry {
        IndexEntry::new(FloEventId::new(actor, counter), 76, 1)
    }

    #[test]
    fn entries_are_deleted() {
        let mut subject = EventIndex::new();
        subject.add(id_entry(1, 1));
        subject.add(id_entry(1, 2));
        subject.add(id_entry(1, 3));

        subject.add(id_entry(2, 1));
        subject.add(id_entry(2, 2));
        subject.add(id_entry(2, 3));

        subject.add(id_entry(3, 1));
        subject.add(id_entry(3, 2));
        subject.add(id_entry(3, 3));

        let mut version_vec = VersionVector::new();
        version_vec.set(FloEventId::new(1, 2));
        version_vec.set(FloEventId::new(2, 3));
        subject.remove_range_inclusive(&version_vec);

        let result: Vec<ConsumerEntries> = subject.get_consumer_start_point(&VersionVector::new()).collect();
        let expected = vec![
            ConsumerEntries {
                start: id_entry(3, 1),
                end: id_entry(3, 3),
            },
            ConsumerEntries {
                start: id_entry(1, 3),
                end: id_entry(1, 3),
            }
        ];

        assert_eq!(expected, result);
    }

    #[test]
    fn get_consumer_start_point_returns_iterator_of_one_entry_per_actor() {
        let mut subject = EventIndex::new();
        subject.add(id_entry(1, 1));
        subject.add(id_entry(1, 2));
        subject.add(id_entry(1, 3));

        subject.add(id_entry(2, 1));
        subject.add(id_entry(2, 2));
        subject.add(id_entry(2, 3));

        subject.add(id_entry(3, 1));
        subject.add(id_entry(3, 2));
        subject.add(id_entry(3, 3));

        let mut version_vector = VersionVector::new();
        version_vector.set(FloEventId::new(1, 1));
        version_vector.set(FloEventId::new(2, 3)); // Should have no entry returned for 2 since version vec already included the highest event for actor 2
        // leave actor 3 out of version vector so we can assert that the entry for 3-1 is returned

        let result: Vec<ConsumerEntries> = subject.get_consumer_start_point(&version_vector).collect();
        let expected = vec![
            ConsumerEntries {
                start: id_entry(1, 2),
                end: id_entry(1, 3)
            },
            ConsumerEntries {
                start: id_entry(3, 1),
                end: id_entry(3, 3)
            }
        ];
        assert_eq!(expected, result);
    }

}


