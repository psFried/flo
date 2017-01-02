mod entry_range_iterator;

pub use self::entry_range_iterator::EntryRangeIterator;
use event::EventId;
use std::cmp::max;


use flo_event::FloEventId;

use std::collections::{BTreeMap, Bound};

//TODO: look into finite state transducers for index https://crates.io/crates/fst or something else to allow an index larger than what fits into memory

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
    greatest_entry: FloEventId
}

impl EventIndex {
    pub fn new(max_events: usize) -> EventIndex {
        EventIndex {
            entries: BTreeMap::new(),
            max_entries: max_events,
            least_entry: FloEventId::new(0, 0),
            greatest_entry: FloEventId::new(0, 0),
        }
    }

    pub fn add(&mut self, new_entry: IndexEntry) -> Option<IndexEntry> {
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

    pub fn get_next_entry(&self, start_after: FloEventId) -> Option<&IndexEntry> {
        self.entries.range(Bound::Excluded(&start_after), Bound::Unbounded).next().map(|(k, v)| v)
    }

    pub fn contains(&self, event_id: FloEventId) -> bool {
        self.entries.contains_key(&event_id)
    }

    pub fn get_greatest_event_id(&self) -> FloEventId {
        self.greatest_entry
    }
}


#[cfg(test)]
mod index_test {
    use super::*;
    use flo_event::{FloEventId, ActorId, EventCounter};

    const ACTOR_ID: ActorId = 1;

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


//////// TODO: delete old code below once new storage engine works

const LOAD_FACTOR: f64 = 0.75;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Entry {
    pub event_id: EventId,
    pub offset: u64,
}

impl Entry {
    pub fn new(event_id: EventId, offset: u64) -> Entry {
        Entry {
            event_id: event_id,
            offset: offset,
        }
    }
}


#[derive(Debug)]
pub struct RingIndex {
    entries: Vec<Option<Entry>>,
    num_entries: usize,
    drop_to_index: usize,
    max_num_events: usize,
    head_index: usize,
    min_event_id: EventId,
    max_event_id: EventId,
}

impl RingIndex {
    pub fn new(initial_size: usize, max_size: usize) -> RingIndex {
        let mut index = RingIndex {
            entries: Vec::with_capacity(initial_size),
            num_entries: 0,
            drop_to_index: 0,
            max_num_events: max_size,
            head_index: 0,
            min_event_id: 1,
            max_event_id: 0,
        };
        index.fill_entries();
        index
    }

    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.num_entries
    }

    pub fn add(&mut self, entry: Entry) -> Option<Entry> {
        trace!("Adding to index: {:?}, total entries: {}",
               entry,
               self.entries.len() + 1);
        self.num_entries += 1;
        let idx = self.get_index(entry.event_id);
        self.ensure_capacity(idx);
        let prev_entry = self.entries[idx];
        if prev_entry.is_some() {
            self.min_event_id += 1;
        }
        self.entries[idx] = Some(entry);
        self.set_new_head(entry.event_id, idx);
        prev_entry
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, event_id: EventId) {
        self.get(event_id).map(|_| {
            let index = self.get_index(event_id);
            self.entries[index] = None;
            self.num_entries -= 1;
        });
    }

    #[allow(dead_code)]
    pub fn get(&self, event_id: EventId) -> Option<Entry> {
        self.get_event_index(event_id).and_then(|index| {
            let matches_event_id = self.entries[index].map(|entry| entry.event_id == event_id).unwrap_or(false);
            if matches_event_id {
                self.entries[index]
            } else {
                None
            }
        })
    }

    pub fn entry_range<'a>(&'a mut self, starting_after: EventId) -> EntryRangeIterator<'a> {
        let event_id = max(starting_after + 1, self.min_event_id);
        let starting_index = self.get_index(event_id);
        let mut max_events = self.max_event_id.saturating_sub(starting_after) as usize;
        max_events = ::std::cmp::min(max_events, self.max_num_events);
        trace!("index entry range: starting_after_event: {}, starting_index: {}, max: {}",
               starting_after,
               starting_index,
               max_events);
        EntryRangeIterator::new(self.entries.as_slice(), max_events, starting_index)
    }

    pub fn get_next_entry(&mut self, event_id: EventId) -> Option<Entry> {
        self.entry_range(event_id).next()
    }

    fn get_event_index(&self, event_id: EventId) -> Option<usize> {
        let index = self.get_index(event_id);
        if index < self.entries.len() {
            Some(index)
        } else {
            None
        }
    }

    fn set_new_head(&mut self, event_id: EventId, index: usize) {
        if event_id > self.max_event_id {
            self.head_index = index;
            self.max_event_id = event_id;
        }
    }

    fn fill_entries(&mut self) {
        let to_add = self.entries.capacity() - self.entries.len();
        for _ in 0..to_add {
            self.entries.push(None);
        }
    }

    fn ensure_capacity(&mut self, index: usize) {
        let current_len = self.entries.len();
        if index >= current_len {
            let new_capacity = index + (index as f64 * LOAD_FACTOR) as usize;
            let max_additional = self.max_num_events - current_len;
            let additional_capacity = ::std::cmp::min(new_capacity - current_len, max_additional);
            debug!("Growing index by: {}, from: {}",
                   additional_capacity,
                   current_len);
            self.entries.reserve_exact(additional_capacity);
            self.fill_entries();
        }
    }

    fn get_index(&self, event_id: EventId) -> usize {
        let idx = event_id as usize - 1;
        idx % self.max_num_events
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use event::EventId;

    fn new_index() -> RingIndex {
        RingIndex::new(5, 100)
    }

    #[test]
    fn get_next_entry_returns_smallest_event_greater_than_the_given_one_after_max_event_has_been_reached() {
        let mut index = RingIndex::new(20, 20);
        for i in 1..25 {
            index.add(Entry::new(i, i));
        }

        let next_event = index.get_next_entry(0);
        assert_eq!(Some(Entry::new(5, 5)), next_event);
    }

    #[test]
    fn add_returns_removed_event_when_an_old_event_is_overwritten() {
        let mut index = RingIndex::new(20, 20);
        for i in 1..20 {
            let add_result = index.add(Entry::new(i, i));
            assert!(add_result.is_none());
        }

        let add_result = index.add(Entry::new(21, 789));
        assert_eq!(Some(Entry::new(1, 1)), add_result);
        let add_result = index.add(Entry::new(22, 789));
        assert_eq!(Some(Entry::new(2, 2)), add_result);
    }

    #[test]
    fn calling_remove_with_an_event_id_that_is_not_in_the_index_does_nothing() {
        let mut index = RingIndex::new(20, 20);
        for i in 10..25 {
            index.add(Entry::new(i, i));
        }
        assert_eq!(15, index.size());
        index.remove(5);
        assert_eq!(15, index.size());

    }

    #[test]
    fn calling_remove_with_an_event_id_greater_than_the_max_does_nothing() {
        let mut index = RingIndex::new(20, 20);
        let num_entries = 10;
        for i in 1..(num_entries + 1) {
            index.add(Entry::new(i, i));
        }

        index.remove(num_entries + 5);
        assert_eq!(num_entries as usize, index.size());
    }

    #[test]
    fn entry_is_removed_from_the_index() {
        let mut index = RingIndex::new(20, 20);
        for i in 1..31 {
            index.add(Entry::new(i, i));
        }
        assert_eq!(30, index.size());

        index.remove(12);
        assert_eq!(29, index.size());
        assert!(index.get(1).is_none());
    }

    #[test]
    fn entry_range_returns_iterator_over_range_of_entries() {
        let mut index = RingIndex::new(20, 20);
        for i in 1..30 {
            index.add(Entry::new(i, i));
        }
        let iterator = index.entry_range(14);
        let iterator_results = iterator.collect::<Vec<Entry>>();
        assert_eq!(15, iterator_results.len());
        assert_eq!(Entry { event_id: 15, offset: 15 }, iterator_results[0]);
        assert_eq!(Entry { event_id: 29, offset: 29 }, iterator_results[14]);
    }

    #[test]
    fn old_entries_are_overwritten_once_max_num_entries_is_reached() {
        let max_capacity = 50usize;
        let mut index = RingIndex::new(10, 50);

        for i in 1..(max_capacity as u64 + 75) {
            index.add(Entry::new(i, 30 * i));
        }
        assert_eq!(max_capacity, index.entries.capacity());
        assert_eq!(124, index.max_event_id);
        assert_eq!(23, index.head_index);
    }

    #[test]
    fn adding_an_entry_increases_the_capacity_of_the_storage_if_needed() {
        let initial_capacity = 5;
        let mut index = RingIndex::new(initial_capacity, 100);
        assert_eq!(initial_capacity, index.entries.capacity());

        index.add(Entry::new(10, 777));

        assert!(index.entries.capacity() > 10);
    }

    #[test]
    fn the_first_entry_gets_added_to_the_first_index_when_event_id_is_one() {
        let mut index = RingIndex::new(10, 10);

        index.add(Entry::new(1, 55));
        assert_eq!(Some(Entry { event_id: 1, offset: 55 }), index.entries[0]);
    }

    #[test]
    fn get_next_entry_wraps_around_the_ring_when_the_end_of_the_buffer_is_filled_with_none() {
        let mut index = RingIndex::new(10, 10);
        index.add(Entry::new(7, 77));
        index.add(Entry::new(11, 111));
        let result = index.get_next_entry(7);
        assert_eq!(Some(Entry { event_id: 11, offset: 111 }), result);
    }

    #[test]
    fn get_next_entry_returns_next_largest_entry_when_last_event_is_at_the_end_of_the_buffer() {
        let mut index = RingIndex::new(10, 10);

        index.add(Entry::new(10, 100));
        index.add(Entry::new(11, 110));

        assert_eq!(Some(Entry { event_id: 10, offset: 100 }), index.entries[9]);
        let result = index.get_next_entry(10);
        assert_eq!(Some(Entry { event_id: 11, offset: 110 }), result);
        assert_eq!(result, index.entries[0]);
    }

    #[test]
    fn get_next_entry_returns_next_largest_entry() {
        let mut idx = new_index();

        idx.add(Entry::new(5, 50));

        let expected = Some(Entry::new(5, 50));
        assert_eq!(expected, idx.get_next_entry(1));
        assert_eq!(expected, idx.get_next_entry(2));
        assert_eq!(expected, idx.get_next_entry(3));
        assert_eq!(expected, idx.get_next_entry(4));

        assert!(idx.get_next_entry(5).is_none());
    }

    #[test]
    fn get_returns_none_when_event_id_is_out_of_bounds() {
        let idx = new_index();
        let result = idx.get(987654);
        assert!(result.is_none());
    }

    #[test]
    fn get_returns_entry_associated_with_the_given_key() {
        let mut idx = new_index();
        let val = 87838457u64;
        let key: EventId = 2;

        idx.add(Entry::new(key, val));

        let result = idx.get(key);
        assert_eq!(Some(Entry::new(key, val)), result);
    }

    #[test]
    fn adding_an_entry_increases_num_entries_by_one() {
        let mut idx = new_index();
        assert_eq!(0, idx.size());
        idx.add(Entry::new(1, 886734));
        assert_eq!(1, idx.size());
        idx.add(Entry::new(2, 8738458));
        assert_eq!(2, idx.size());
    }
}
