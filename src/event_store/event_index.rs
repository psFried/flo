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
    head_event_id: EventId,
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
            head_event_id: 0,
        };
        index.fill_entries();
        index
    }

    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    pub fn add(&mut self, event_id: EventId, offset: usize) {
        self.num_entries += 1;
        let idx = EventIndex::get_relative_index(self.head_index, self.head_event_id, event_id, self.max_num_events);
        self.ensure_capacity(idx);
        self.entries[idx] = Some(Entry::new(event_id, offset));
        self.set_new_head(event_id, idx);
    }

    pub fn get(&self, event_id: EventId) -> Option<Entry> {
        let index = self.get_index(event_id);
        if index < self.entries.len() {
            self.entries[index]
        } else {
            None
        }
    }

    pub fn get_next_entry(&mut self, event_id: EventId) -> Option<Entry> {
        None
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
        EventIndex::get_relative_index(self.head_index, self.head_event_id, event_id, self.max_num_events)
    }

    fn get_relative_index(head_index: usize,
        head_event_id: EventId,
        new_event_id: EventId,
        max_exclusive: usize) -> usize {

        if new_event_id > head_event_id {
            let event_id_diff = (new_event_id - head_event_id) as usize;
            let index = head_index + event_id_diff;
            index % max_exclusive
        } else {
            let event_id_diff = (head_event_id - new_event_id) as usize;
            if event_id_diff > head_index {
                let wrapping_amount = event_id_diff - head_index;
                max_exclusive - wrapping_amount
            } else {
                head_index - event_id_diff
            }
        }
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
    fn old_entries_are_overwritten_once_max_num_entries_is_reached() {
        let max_capacity = 50usize;
        let mut index = EventIndex::new(10, 50);

        for i in 1..(max_capacity as u64 + 75) {
            index.add(i, 30 * i as usize);
        }
        assert_eq!(max_capacity, index.entries.capacity());
        assert_eq!(124, index.head_event_id);
        assert_eq!(24, index.head_index);
    }

    #[test]
    fn adding_an_entry_increases_the_capacity_of_the_storage_if_needed() {
        let initial_capacity = 5;
        let mut index = EventIndex::new(initial_capacity, 100);
        assert_eq!(initial_capacity, index.entries.capacity());

        index.add(10, 777);

        assert!(index.entries.capacity() > 10);
    }

    #[test]
    fn get_relative_index_wraps_around_0_when_new_event_id_is_less_than_head_id() {
        let head_index = 1;
        let head_event_id: EventId = 555;
        let new_event_id: EventId = 550;
        let max = 1000;

        let result = EventIndex::get_relative_index(head_index,
                head_event_id,
                new_event_id,
                max);
        assert_eq!(996, result);
    }

    #[test]
    fn get_relative_index_wraps_around_max_value_when_new_event_id_is_greater_than_head_id() {
        let head_index = 8;
        let head_event_id: EventId = 555;
        let new_event_id: EventId = 557;
        let max = 10;

        let result = EventIndex::get_relative_index(head_index,
                head_event_id,
                new_event_id,
                max);
        assert_eq!(0, result);
    }

    #[test]
    fn get_relative_index_subtracts_difference_when_new_event_id_is_less_than_head_event_id() {
        let head_index = 10;
        let head_event_id: EventId = 66;
        let new_event_id: EventId = 60;
        let max = 9999;

        let result = EventIndex::get_relative_index(head_index,
                head_event_id,
                new_event_id,
                max);
        assert_eq!(4, result);
    }

    #[test]
    fn get_relative_index_adds_difference_between_head_and_new_event_ids_if_under_max() {
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

        idx.add(key, val);

        let result = idx.get(key);
        assert_eq!(Some(Entry::new(key, val)), result);
    }

    #[test]
    fn adding_an_entry_increases_num_entries_by_one() {
        let mut idx = new_index();
        assert_eq!(0, idx.num_entries());
        idx.add(1, 886734);
        assert_eq!(1, idx.num_entries());
        idx.add(2, 8738458);
        assert_eq!(2, idx.num_entries());
    }
}
