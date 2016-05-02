use event::EventId;


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
    load_factor: f64,
    length: usize,
    drop_to: EventId,
}

impl EventIndex {

    pub fn new(initial_size: usize, load_factor: f64) -> EventIndex {
        let mut index = EventIndex {
            entries: Vec::with_capacity(initial_size),
            min_event: 0,
            max_event: 0,
            length: 0,
            drop_to: 0,
            load_factor: load_factor,
        };
        index.fill_entries();
        index
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn add(&mut self, event_id: EventId, offset: usize) {
        self.length += 1;
        self.max_event = event_id;
        let idx = self.get_index(event_id);
        self.ensure_capacity_for(idx);
        self.entries[idx] = Some(Entry::new(event_id, offset));
    }

    pub fn get(&self, event_id: EventId) -> Option<Entry> {
        self.entries[self.get_index(event_id)]
    }

    pub fn get_next_entry(&mut self, event_id: EventId) -> Option<Entry> {
        let mut idx = self.get_index(event_id) + 1;
        if idx < self.max_event as usize {
            let mut entry = self.entries[idx];
            while entry.is_none() && idx < self.max_event as usize {
                idx += 1;
                entry = self.entries[idx];
            }
            entry
        } else {
            None
        }
    }

    pub fn drop(&mut self, min_event_id: EventId) {
        self.drop_to = min_event_id;
    }

    fn ensure_capacity_for(&mut self, index: usize) {
        if index >= self.entries.len() {
            let additional_capacity = index + (index as f64 * self.load_factor) as usize;
            self.reallocate_entries(additional_capacity);
        }
    }

    fn reallocate_entries(&mut self, additional_capacity: usize) {
        if self.should_drop() {
            let new_capacity = self.entries.capacity() + additional_capacity;
            let mut new_entries: Vec<Option<Entry>> = Vec::with_capacity(new_capacity);
            let drop_to = self.drop_to;
            let new_min_entry = self.get_next_entry(drop_to);
            println!("Dropping old entries. New min entry: {:?}", new_min_entry);

            self.min_event = new_min_entry.map(|entry| entry.event_id)
                    .unwrap_or(self.drop_to);

            let num_to_drop = new_min_entry.map(|entry| {
                entry.event_id.saturating_sub(1) as usize
            }).unwrap_or(0);
            for opt_entry in self.entries.iter().skip(num_to_drop) {
                new_entries.push(*opt_entry);
            }
            println!("Reallocated entries - capacities: old: {}, new: {}",
                    self.entries.capacity(),
                    new_capacity);
            self.entries = new_entries;
        } else {
            println!("Growing index by: {}, from: {}", additional_capacity, self.entries.capacity());
            self.entries.reserve(additional_capacity);
        }
        self.fill_entries();
        println!("Reallocated index to: cap: {}, len: {}", self.entries.capacity(), self.entries.len());
    }

    fn should_drop(&self) -> bool {
        self.drop_to > self.min_event
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
}


#[cfg(test)]
mod test {
    use super::*;
    use event::EventId;

    fn new_index() -> EventIndex {
        EventIndex::new(5, 0.7)
    }

    #[test]
    fn old_events_should_be_dropped_when_the_storage_is_reallocated() {
        let mut index = new_index();

        for i in 1..5 {
            index.add(i, 50 * i as usize);
        }
        index.drop(3);

        assert_eq!(Some(Entry::new(1, 50)), index.entries[0]);
        for i in 5..12 {
            index.add(i, 50 * i as usize);
        }

        assert_eq!(Some(Entry::new(4, 200)), index.entries[0]);
    }

    #[test]
    fn adding_an_entry_should_reallocate_if_new_capacity_is_needed() {
        let initial_size = 5;
        let mut index = EventIndex::new(initial_size, 0.1);

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
        assert_eq!(0, idx.len());
        idx.add(1, 886734);
        assert_eq!(1, idx.len());
        idx.add(2, 8738458);
        assert_eq!(2, idx.len());
    }
}
