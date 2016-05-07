mod entry_range_iterator;

pub use self::entry_range_iterator::EntryRangeIterator;
use event::EventId;

const LOAD_FACTOR: f64 = 0.75;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Entry {
    pub event_id: EventId,
    pub offset: usize,
}

impl Entry {
    pub fn new(event_id: EventId, offset: usize) -> Entry {
        Entry {
            event_id: event_id,
            offset: offset,
        }
    }
}


#[derive(Debug)]
pub struct RingIndex {
    entries: Vec<Option<Entry>>,
    min_event: EventId,
    max_event: EventId,
    num_entries: usize,
    drop_to_event: EventId,
    drop_to_index: usize,
    max_num_events: usize,
    head_index: usize,
    head_event_id: EventId,
}

impl RingIndex {

    pub fn new(initial_size: usize, max_size: usize) -> RingIndex {
        let mut index = RingIndex {
            entries: Vec::with_capacity(initial_size),
            min_event: 0,
            max_event: 0,
            num_entries: 0,
            drop_to_event: 0,
            drop_to_index: 0,
            max_num_events: max_size,
            head_index: 0,
            head_event_id: 1,
        };
        index.fill_entries();
        index
    }

    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    pub fn add(&mut self, entry: Entry) {
        self.num_entries += 1;
        let idx = self.get_index(entry.event_id);
        self.ensure_capacity(idx);
        self.entries[idx] = Some(entry);
        self.set_new_head(entry.event_id, idx);
    }

    pub fn get(&self, event_id: EventId) -> Option<Entry> {
        let index = self.get_index(event_id);
        if index < self.entries.len() {
            self.entries[index]
        } else {
            None
        }
    }

    pub fn entry_range<'a>(&'a mut self, starting_after: EventId) -> EntryRangeIterator<'a> {
        let starting_index = self.get_index(starting_after + 1);
        let mut max_events = self.head_event_id.saturating_sub(starting_after) as usize;
        max_events = ::std::cmp::min(max_events, self.max_num_events);
        EntryRangeIterator::new(self.entries.as_slice(), max_events, starting_index)
    }

    pub fn get_next_entry(&mut self, event_id: EventId) -> Option<Entry> {
        self.entry_range(event_id).next()
    }

    pub fn drop(&mut self, min_event_id: EventId) {
        self.drop_to_event = min_event_id;
    }

    fn set_new_head(&mut self, event_id: EventId, index: usize) {
        if event_id > self.head_event_id {
            self.head_index = index;
            self.head_event_id = event_id;
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
    fn entry_range_returns_iterator_over_range_of_entries() {
        let mut index = RingIndex::new(20, 20);
        for i in 1..30 {
            index.add(Entry::new(i, i as usize));
        }
        let iterator = index.entry_range(14);
        let iterator_results = iterator.collect::<Vec<Entry>>();
        assert_eq!(15, iterator_results.len());
        assert_eq!(Entry{event_id: 15, offset: 15}, iterator_results[0]);
        assert_eq!(Entry{event_id: 29, offset: 29}, iterator_results[14]);
    }

    #[test]
    fn old_entries_are_overwritten_once_max_num_entries_is_reached() {
        let max_capacity = 50usize;
        let mut index = RingIndex::new(10, 50);

        for i in 1..(max_capacity as u64 + 75) {
            index.add(Entry::new(i, 30 * i as usize));
        }
        assert_eq!(max_capacity, index.entries.capacity());
        assert_eq!(124, index.head_event_id);
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
        println!("234 index: {:?}", index);
        assert_eq!(Some(Entry{event_id: 1, offset: 55}), index.entries[0]);
    }

    #[test]
    fn get_next_entry_wraps_around_the_ring_when_the_end_of_the_buffer_is_filled_with_None() {
        let mut index = RingIndex::new(10, 10);
        index.add(Entry::new(7, 77));
        index.add(Entry::new(11, 111));
        let result = index.get_next_entry(7);
        assert_eq!(Some(Entry{event_id: 11, offset: 111}), result);
    }

    #[test]
    fn get_next_entry_returns_next_largest_entry_when_last_event_is_at_the_end_of_the_buffer() {
        let mut index = RingIndex::new(10, 10);

        index.add(Entry::new(10, 100));
        index.add(Entry::new(11, 110));

        assert_eq!(Some(Entry{event_id: 10, offset: 100}), index.entries[9]);
        let result = index.get_next_entry(10);
        assert_eq!(Some(Entry{event_id: 11, offset: 110}), result);
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
        let mut idx = new_index();
        let result = idx.get(987654);
        assert!(result.is_none());
    }

    #[test]
    fn get_returns_entry_associated_with_the_given_key() {
        let mut idx = new_index();
        let val = 87838457usize;
        let key: EventId = 2;

        idx.add(Entry::new(key, val));

        let result = idx.get(key);
        assert_eq!(Some(Entry::new(key, val)), result);
    }

    #[test]
    fn adding_an_entry_increases_num_entries_by_one() {
        let mut idx = new_index();
        assert_eq!(0, idx.num_entries());
        idx.add(Entry::new(1, 886734));
        assert_eq!(1, idx.num_entries());
        idx.add(Entry::new(2, 8738458));
        assert_eq!(2, idx.num_entries());
    }
}
