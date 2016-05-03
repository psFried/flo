use event::EventId;

const LOAD_FACTOR: f64 = 0.75;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Entry {
    pub event_id: EventId,
    pub offset: usize,
}

impl Entry {
    fn new(event_id: EventId, offset: usize) -> Entry {
        Entry {
            event_id: event_id,
            offset: offset,
        }
    }
}


pub struct EventIndex {
    entries: Vec<Option<Entry>>,
    min_event: EventId,
    max_event: EventId,
    num_entries: usize,
    drop_to_event: EventId,
    drop_to_index: usize,
    max_num_events: usize,
    head_index: usize,
}

impl EventIndex {

    pub fn new(initial_size: usize, max_size: usize) -> EventIndex {
        let mut index = EventIndex {
            entries: Vec::with_capacity(initial_size),
            min_event: 0,
            max_event: 0,
            num_entries: 0,
            drop_to_event: 0,
            drop_to_index: 0,
            max_num_events: max_size,
            head_index: 0,
        };
        index.fill_entries();
        index
    }

    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    pub fn add(&mut self, event_id: EventId, offset: usize) {
        self.num_entries += 1;
        self.max_event = event_id;
        let idx = self.ensure_capacity_for(event_id);
        self.entries[idx] = Some(Entry::new(event_id, offset));
    }

    pub fn get(&self, event_id: EventId) -> Option<Entry> {
        None
    }

    pub fn get_next_entry(&mut self, event_id: EventId) -> Option<Entry> {
        None
    }

    pub fn drop(&mut self, min_event_id: EventId) {
        self.drop_to_event = min_event_id;
    }


    fn can_grow(&self) -> bool {
        self.entries.len() < self.max_num_events
    }

    fn grow_storage(&mut self, ensure_index: usize) {
        let current_len = self.entries.len();

    }

    fn get_new_capacity(&self, ensure_index: usize) -> usize {
        unimplemented!()
    }

    fn drop_entries(&mut self) {

    }


    fn ensure_capacity_for(&mut self, event_id: EventId) -> usize {
        match self.entries[self.head_index] {
            Some(entry) => {
                let head_event_id = entry.event_id;
                if head_event_id > event_id {
                    0
                } else {
                    0
                }
            },
            None => self.head_index
        }
    }

    fn should_drop(&self) -> bool {
        self.drop_to_event > self.min_event
    }

    fn fill_entries(&mut self) {
        let to_add = self.entries.capacity() - self.entries.len();
        for _ in 0..to_add {
            self.entries.push(None);
        }
    }

    fn get_index(&self, event_id: EventId) -> usize {
        (event_id - self.min_event - 1) as usize
    }

    fn get_relative_index(head_index: usize,
        head_event_id: EventId,
        new_event_id: EventId,
        max_exclusive: usize) -> usize {

            0

        }
}


#[cfg(test)]
mod test {
    use super::*;
    use event::EventId;

    fn new_index() -> EventIndex {
        EventIndex::new(5, 100)
    }

    #[test]
    fn get_relative_index_add_difference_between_head_and_new_event_ids_if_under_max() {
        let head_index = 0;
        let head_event_id: EventId = 1;
        let new_event_id: EventId = 2;
        let max = 9999;

        let result = EventIndex::get_relative_index(head_index,
                head_event_id,
                new_event_id,
                max);
        assert_eq!(1, result);
    }

    #[test]
    fn adding_an_entry_should_reallocate_if_new_capacity_is_needed() {
        let initial_size = 5;
        let mut index = EventIndex::new(initial_size, 100);

        index.add(75, 8888);
        assert_eq!(Some(Entry::new(75, 8888)), index.get(75));
        assert!(index.entries.len() >= 75);
    }

    #[test]
    fn get_next_entry_returns_next_largest_entry() {
        let mut idx = new_index();

        idx.add(5, 50);

        let expected = Some(Entry::new(5, 50));
        assert_eq!(expected, idx.get_next_entry(1));
        assert_eq!(expected, idx.get_next_entry(2));
        assert_eq!(expected, idx.get_next_entry(3));
        assert_eq!(expected, idx.get_next_entry(4));

        assert!(idx.get_next_entry(5).is_none());
    }

    #[test]
    fn the_capacity_and_length_of_the_internal_storage_vector_are_always_the_same() {
        let mut index = new_index();
        assert_eq!(index.entries.capacity(), index.entries.len());

        for i in 1..70 {
            index.add(i, 40 * i as usize);
        }
        assert_eq!(index.entries.capacity(), index.entries.len());
    }

    #[test]
    fn get_returns_entry_associated_with_the_given_key() {
        let mut idx = new_index();
        let val = 87838457usize;
        let key: EventId = 2;

        idx.add(key, val);

        let result = idx.get(key);
        assert_eq!(Some(Entry::new(key, val)), result);
    }

    #[test]
    fn adding_an_entry_increases_length_by_one() {
        let mut idx = new_index();
        assert_eq!(0, idx.num_entries());
        idx.add(1, 886734);
        assert_eq!(1, idx.num_entries());
        idx.add(2, 8738458);
        assert_eq!(2, idx.num_entries());
    }
}
