use std::sync::Arc;
use std::collections::{BTreeMap, Bound};

use flo_event::{FloEventId, FloEvent, OwnedFloEvent};
use server::MemoryLimit;

pub struct Cache {
    entries: BTreeMap<FloEventId, Arc<OwnedFloEvent>>,
    least_event_id: FloEventId,
    greatest_event_id: FloEventId,
    last_evicted_id: FloEventId,
    max_entries: usize,
    max_memory: usize,
    current_memory: usize,
}

impl Cache {
    pub fn new(max_events: usize, max_memory: MemoryLimit) -> Cache {
        Cache {
            entries: BTreeMap::new(),
            least_event_id: FloEventId::zero(),
            last_evicted_id: FloEventId::zero(),
            greatest_event_id: FloEventId::zero(),
            max_entries: max_events,
            max_memory: max_memory.as_bytes(),
            current_memory: 0,
        }
    }

    pub fn do_with_range<F>(&self, start_exclusive: FloEventId, limit: usize, mut fun: F) where F: FnMut((FloEventId, Arc<OwnedFloEvent>)) {
        let iter = self.entries.range(Bound::Excluded(&start_exclusive), Bound::Unbounded).take(limit);
        for (k, v) in iter {
            fun((*k, v.clone()));
        }
    }

    pub fn insert(&mut self, event: OwnedFloEvent) -> Arc<OwnedFloEvent> {
        let event_id = event.id;
        if self.least_event_id.is_zero() {
            self.least_event_id = event_id;
        }

        if event_id > self.greatest_event_id {
            self.greatest_event_id = event_id;
        }

        let event_size = size_of(&event);

        while (self.entries.len() + 1 >= self.max_entries) || (self.current_memory + event_size > self.max_memory) {
            self.remove_oldest_entry();
        }

        self.current_memory += event_size;
        trace!("Cache inserted event: {:?}, cache memory usage: {:?}", event.id, self.current_memory);
        let event_rc = Arc::new(event);
        self.entries.insert(event_id, event_rc.clone());
        event_rc
    }

    pub fn min(&self) -> FloEventId {
        self.least_event_id
    }

    pub fn max(&self) -> FloEventId {
        self.greatest_event_id
    }

    pub fn last_evicted_id(&self) -> FloEventId {
        self.last_evicted_id
    }

    fn remove_oldest_entry(&mut self) {
        self.entries.keys().take(1).cloned().next().map(|id| {
            self.entries.remove(&id).map(|event| {
                self.current_memory -= size_of(&*event);
                trace!("Cache evicted event: {:?}", id);
            });
        });
    }
}

fn size_of(event: &OwnedFloEvent) -> usize {
    ::std::mem::size_of::<OwnedFloEvent>() +
            ::std::mem::size_of::<Arc<OwnedFloEvent>>() +
            event.data_len() as usize +
            event.namespace.len()
}

