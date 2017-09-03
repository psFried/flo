
use std::collections::BTreeMap;

use event::{EventCounter, ActorId};
use super::SegmentNum;


#[derive(Debug, PartialEq, Clone)]
pub struct IndexEntry {
    pub counter: EventCounter,
    pub segment: SegmentNum,
    pub file_offset: usize,
}

impl IndexEntry {
    pub fn new(counter: EventCounter, segment: SegmentNum, file_offset: usize) -> IndexEntry {
        // IndexEntries only make sense for non-zero segment numbers
        debug_assert!(segment.is_set());
        IndexEntry {
            counter: counter,
            segment: segment,
            file_offset: file_offset,
        }
    }
}

pub struct PartitionIndex {
    partition_num: ActorId,
    entries: Vec<InternalEntry>,
    highest_counter: EventCounter,
    lowest_counter: EventCounter,
    pivot_index: usize,
}

fn wrap_index(index: usize, cap: usize) -> usize {
    // just take the lowest bits of index
    // this only works because cap is guaranteed to be a power of 2
    index & (cap - 1)
}

impl PartitionIndex {

    pub fn new(partition_num: ActorId) -> PartitionIndex {
        PartitionIndex::with_capacity(partition_num, 1024)
    }

    fn with_capacity(partition_num: ActorId, capacity: usize) -> PartitionIndex {
        assert!(capacity.is_power_of_two());
        PartitionIndex {
            partition_num: partition_num,
            entries: vec![InternalEntry::default(); capacity],
            highest_counter: 0,
            lowest_counter: 0,
            pivot_index: 0,
        }
    }

    pub fn append(&mut self, entry: IndexEntry) {
        let internal = InternalEntry {
            segment: entry.segment,
            file_offset: entry.file_offset,
        };
        let index = self.get_insert_index(entry.counter);
        if entry.counter > self.highest_counter {
            self.highest_counter = entry.counter;
        }
        unsafe {
            self.entries.get_unchecked_mut(index).set(entry.segment, entry.file_offset)
        }
    }

    pub fn remove_through(&mut self, remove_through: EventCounter) {
        match self.get_read_index(remove_through) {
            Some(index) => {
                // we're just removing a subset of entries
                self.pivot_index = index + 1;
                self.lowest_counter = remove_through + 1;
            }
            None => {
                // We be clearing this whole thing out
                self.pivot_index = 0;
                unsafe {
                    // This _seems_ to be the easiest way to zero out memory in rust...
                    let len = self.entries.len();
                    let ptr = self.entries.as_mut_ptr();
                    ::std::ptr::write_bytes(ptr, 0, len);
                }
            }
        }
    }

    pub fn get_next_entry(&self, start_exclusive: EventCounter) -> Option<IndexEntry> {
        let maybe_index = self.get_read_index(start_exclusive + 1);
        if maybe_index.is_none() {
            return None;
        }

        let mut index = maybe_index.unwrap();

        if index >= self.pivot_index {
            while index < self.cap() {
                let internal = self.entries[index];
                if internal.is_set() {
                    let counter = index as EventCounter + self.counter_offset();
                    return Some(IndexEntry {
                        counter: counter,
                        segment: internal.segment,
                        file_offset: internal.file_offset,
                    })
                } else {
                    index += 1;
                }
            }
            // wrap around unless the pivot happens to be on the first index
            if self.pivot_index > 0 {
                index = 0;
            }
        }

        while index < self.pivot_index {
            let internal = self.entries[index];
            if internal.is_set() {
                let counter = ::std::cmp::max(index as EventCounter, 1) + start_exclusive;
                return Some(IndexEntry {
                    counter: counter,
                    segment: internal.segment,
                    file_offset: internal.file_offset,
                })
            } else {
                index += 1;
            }
        }

        None
    }

    fn cap(&self) -> usize {
        self.entries.len()
    }

    /// Returns the distance between the first event counter and the first set index
    fn counter_offset(&self) -> EventCounter {
        ::std::cmp::max(self.lowest_counter - self.pivot_index as EventCounter, 1)
    }

    fn get_read_index(&self, counter: EventCounter) -> Option<usize> {
        if counter < self.lowest_counter {
            Some(self.pivot_index)
        } else if counter > self.highest_counter {
            None
        } else {
            let unwrapped = (counter - self.counter_offset()) as usize;
            let index = wrap_index(unwrapped, self.cap());
            Some(index)
        }
    }

    /// returns a valid index at which to put the element with the given counter.
    /// This will resize the index if required, so the returned index is always guaranteed to be valid
    fn get_insert_index(&mut self, counter: EventCounter) -> usize {
        let unwrapped = (counter - self.counter_offset()) as usize;
        let index = wrap_index(unwrapped, self.entries.len());

        if index != unwrapped && index >= self.pivot_index {
            // we've wrapped the index around, but it's comes after our pivot point, so we need to resize
            self.grow();
            self.get_insert_index(counter)
        } else {
            index
        }
    }

    fn grow(&mut self) {
        // cap should always be a power of 2
        let new_cap = self.cap() * 2;
        let mut new_entries = Vec::with_capacity(new_cap);
        unsafe {
            new_entries.set_len(new_cap);
        }

        // copy over all the entries, starting with the earliest ones
        let dst_len = self.cap() - self.pivot_index;
        {
            let src = &self.entries[self.pivot_index..];
            let dst = &mut new_entries[0..dst_len];
            dst.copy_from_slice(src);
        }

        if self.pivot_index > 0 {
            // the end of the list wraps around, so we'll get the ones at the lower indexes now
            let src = &self.entries[0..self.pivot_index];
            let moar_len = dst_len + self.pivot_index;
            let dst = &mut new_entries[dst_len..moar_len];
            dst.copy_from_slice(src);
        }
        ::std::mem::swap(&mut self.entries, &mut new_entries);
        self.pivot_index = 0;
    }
}

#[derive(Debug, PartialEq, Copy, Clone, Default)]
struct InternalEntry {
    segment: SegmentNum,
    file_offset: usize,
}

impl InternalEntry {
    fn is_set(&self) -> bool {
        self.segment.is_set()
    }

    fn set(&mut self, segment: SegmentNum, file_offset: usize) {
        self.segment = segment;
        self.file_offset = file_offset;
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use event::{EventCounter, ActorId};


    #[test]
    fn append_and_retrieve_entry() {
        let mut index = PartitionIndex::new(5);
        let entry = entry(1);
        index.append(entry.clone());

        let result = index.get_next_entry(0);
        assert_eq!(Some(entry), result);
    }

    #[test]
    fn append_entry_with_a_gap_and_retrieve() {
        let mut index = PartitionIndex::new(5);
        index.append(entry(1));
        index.append(entry(2));
        index.append(entry(5));

        let result = index.get_next_entry(2);
        assert_eq!(Some(entry(5)), result);

        let result = index.get_next_entry(1);
        assert_eq!(Some(entry(2)), result);
    }

    #[test]
    fn remove_range_removes_inclusive_range() {
        let mut index = PartitionIndex::new(5);
        index.append(entry(1));
        index.append(entry(2));
        index.append(entry(5));

        index.remove_through(2);
        assert_next_counter_equals(&index, 0, 5);
    }

    #[test]
    fn entries_wrap_around_when_there_is_capacity_at_the_front_and_then_doubles_in_size() {
        let mut index = PartitionIndex::with_capacity(99, 8);
        index.append(entry(1));
        index.append(entry(2));
        index.append(entry(3));
        index.append(entry(4));
        index.append(entry(5));
        index.append(entry(6));
        index.append(entry(7));

        index.remove_through(3);

        assert_next_counter_equals(&index, 0, 4);

        index.append(entry(8));
        // 9 and 10 should be written at the front of the vec
        index.append(entry(9));
        index.append(entry(10));

        assert_next_counter_equals(&index, 7, 8);
        assert_next_counter_equals(&index, 8, 9);
        assert_next_counter_equals(&index, 9, 10);


        // now add more so the index is forced to resize
        index.append(entry(11));
        index.append(entry(12));
        index.append(entry(13));

        assert_next_counter_equals(&index, 0, 4);
        assert_next_counter_equals(&index, 7, 8);
        assert_next_counter_equals(&index, 8, 9);
        assert_next_counter_equals(&index, 9, 10);
        assert_next_counter_equals(&index, 12, 13);
    }

    #[test]
    fn add_and_remove_entries_while_maintaining_a_constant_size() {
        let mut index = PartitionIndex::with_capacity(99, 16);

        // load it up with 15 entries
        for i in 1..16 {
            index.append(entry(i));
        }


        for _ in 0..50 {
            let count = 3;
            let to_remove = index.lowest_counter + count;
            index.remove_through(to_remove);

            for _ in 0..count {
                let counter = index.highest_counter + 1;
                index.append(entry(counter));
            }
        }

        assert_eq!(16, index.cap());
    }

    #[test]
    fn index_doubles_in_size_as_it_grows() {
        let mut old_len = 8;
        let mut expected_len = old_len;
        let mut index = PartitionIndex::with_capacity(99, old_len);

        let mut counter = 1;
        for _ in 0..6 {
            for _ in 0..old_len {
                index.append(entry(counter));
                counter += 1;
            }
            assert_eq!(expected_len, index.cap());
            old_len = expected_len;
            expected_len *= 2;
        }
        // make sure the first entry is still there
        assert_next_counter_equals(&index, 0, 1)
    }

    fn assert_next_counter_equals(index: &PartitionIndex, next_input: EventCounter, expected_counter: EventCounter) {
        match index.get_next_entry(next_input) {
            Some(entry) => {
                assert_eq!(expected_counter, entry.counter, "expected next index entry for argument: {} to be: {}, but got: {:?}", next_input, expected_counter, entry)
            }
            None => {
                panic!("expected next index entry for argument: {} to be: {} but got None", next_input, expected_counter)
            }
        }

    }

    fn entry(counter: EventCounter) -> IndexEntry {
        IndexEntry::new(counter, SegmentNum(2), counter as usize)
    }

}
