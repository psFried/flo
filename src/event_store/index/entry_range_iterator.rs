use super::Entry;
use event::EventId;
use std::cmp::min;


pub struct EntryRangeIterator<'a> {
    entries: &'a [Option<Entry>],
    max_iterations: usize,
    current_index: usize,
    iterations: usize,
}

impl <'a> EntryRangeIterator<'a> {
    pub fn new<'b>(entries: &'b [Option<Entry>], max_iterations: usize, start_index: usize) -> EntryRangeIterator<'b> {
        EntryRangeIterator {
            entries: entries,
            max_iterations: min(max_iterations, entries.len()),
            current_index: start_index,
            iterations: 0,
        }
    }

    fn advance_index(&mut self) {
        self.iterations += 1;
        self.current_index += 1;
        if self.current_index == self.entries.len() {
            self.current_index = 0;
        }
    }

    fn should_continue(&self) -> bool {
        self.current_index < self.entries.len() && self.iterations < self.max_iterations
    }
}

impl <'a> Iterator for EntryRangeIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        while self.should_continue() {
            let entry = self.entries[self.current_index];
            self.advance_index();
            if entry.is_some() {
                return entry;
            }
        }
        return None;
    }
}

#[cfg(test)]
mod test {
    use event_store::index::Entry;
    use event::EventId;
    use super::*;

    #[test]
    fn iterator_does_not_provide_the_same_entry_twice() {
        let entries = vec![
            Some(Entry{event_id: 6, offset: 1}),
            Some(Entry{event_id: 7, offset: 1}),
            Some(Entry{event_id: 8, offset: 1}),
            Some(Entry{event_id: 9, offset: 1}),
            Some(Entry{event_id: 10, offset: 1}),
        ];
        let mut iterator = EntryRangeIterator::new(entries.as_slice(), 99, 3);

        let result = iterator.count();
        assert_eq!(5, result);
    }

    #[test]
    fn iterator_wraps_around_to_beginning_of_slice_after_the_end_is_reached() {
        let entries = vec![
            Some(Entry{event_id: 6, offset: 1}),
            Some(Entry{event_id: 7, offset: 1}),
            Some(Entry{event_id: 8, offset: 1}),
            Some(Entry{event_id: 9, offset: 1}),
            Some(Entry{event_id: 10, offset: 1}),
        ];
        let mut iterator = EntryRangeIterator::new(entries.as_slice(), 8, 4);

        let result = iterator.next();
        assert_eq!(entries[4], result);

        let result = iterator.next();
        assert_eq!(entries[0], result);
    }

    #[test]
    fn next_returns_none_once_max_num_iterations_is_passed() {
        let entries = vec![
            Some(Entry{event_id: 6, offset: 1}),
            Some(Entry{event_id: 7, offset: 1}),
            Some(Entry{event_id: 8, offset: 1}),
            Some(Entry{event_id: 9, offset: 1}),
            Some(Entry{event_id: 10, offset: 1}),
        ];
        let mut iterator = EntryRangeIterator::new(entries.as_slice(), 3, 0);

        let result = iterator.last();
        assert_eq!(entries[2], result);
    }

    #[test]
    fn next_returns_next_sequential_event_in_slice() {
        let entries = vec![
            Some(Entry{event_id: 6, offset: 1}),
            Some(Entry{event_id: 7, offset: 2})
        ];
        let mut iterator = EntryRangeIterator::new(entries.as_slice(), 99, 0);
        let result = iterator.next();
        assert_eq!(entries[0], result);
        let result = iterator.next();
        assert_eq!(entries[1], result);
    }

    #[test]
    fn next_returns_none_if_slice_is_empty() {
        let entries: Vec<Option<Entry>> = vec!();
        let mut iterator = EntryRangeIterator::new(entries.as_slice(), 99, 0);
        let result = iterator.next();
        assert!(result.is_none());
    }
}
