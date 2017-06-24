use std::sync::Arc;
use std::collections::{BTreeMap, Bound};
use std::collections::btree_map::Range;

use event::{FloEventId, FloEvent, OwnedFloEvent};
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

    /// Ok, this is a little weird since caching is handled at higher level than storage...
    /// When we initialize the cache, we need to know if there are pre-existing events that
    /// aren't cached. This is the purpose of the `greatest_uncached_event` parameter.
    pub fn new(max_events: usize, max_memory: MemoryLimit, greatest_uncached_event: FloEventId) -> Cache {
        info!("Initializing event cache with max_events: {}, max_memory: {:?}, greatest_uncached_event: {}", max_events, max_memory, greatest_uncached_event);
        Cache {
            entries: BTreeMap::new(),
            least_event_id: FloEventId::zero(),
            last_evicted_id: greatest_uncached_event,
            greatest_event_id: FloEventId::zero(),
            max_entries: max_events,
            max_memory: max_memory.as_bytes(),
            current_memory: 0,
        }
    }

    pub fn do_with_range<T>(&self, start_exclusive: FloEventId, mut fun: T) where T: FnMut(FloEventId, &Arc<OwnedFloEvent>) -> bool {
        let iter = self.entries.range((Bound::Excluded(&start_exclusive), Bound::Unbounded));
        for (k, v) in iter {
            if !fun(*k, v) {
                break;
            }
        }
    }

    pub fn iter(&self, start_exclusive: FloEventId) -> Range<FloEventId, Arc<OwnedFloEvent>> {
        self.entries.range((Bound::Excluded(&start_exclusive), Bound::Unbounded))
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

        while (self.entries.len() >= self.max_entries) || (self.current_memory + event_size > self.max_memory) {
            self.remove_oldest_entry();
        }

        self.current_memory += event_size;
        trace!("Cache inserted event: {}, cache memory usage: {} bytes", event.id, self.current_memory);
        let event_rc = Arc::new(event);
        self.entries.insert(event_id, event_rc.clone());
        event_rc
    }

    pub fn last_evicted_id(&self) -> FloEventId {
        self.last_evicted_id
    }

    fn remove_oldest_entry(&mut self) {
        self.entries.keys().take(1).cloned().next().map(|id| {
            self.entries.remove(&id).map(|event| {
                self.current_memory -= size_of(&*event);
                self.last_evicted_id = event.id;
                trace!("Cache evicted event: {}", id);
            });
        });
    }
}

/// This is used to determine the amount of memory will use in the cache. It doesn't have to be perfect
fn size_of(event: &OwnedFloEvent) -> usize {
    ::std::mem::size_of::<OwnedFloEvent>() +
            ::std::mem::size_of::<Arc<OwnedFloEvent>>() +
            event.data_len() as usize +
            event.namespace.len()
}



#[cfg(test)]
mod test {
    use super::*;
    use event::{FloEventId, ActorId, EventCounter, OwnedFloEvent};
    use server::{MemoryLimit, MemoryUnit};

    #[test]
    fn cache_evicts_events_after_the_max_number_of_events_is_reached() {
        let max = 5;
        let mut subject = Cache::new(max, MemoryLimit::new(5, MemoryUnit::Megabyte), FloEventId::zero());

        subject.insert(event(1, 1));
        subject.insert(event(1, 2));
        subject.insert(event(1, 3));
        subject.insert(event(1, 4));
        subject.insert(event(1, 5));

        assert_eq!(FloEventId::zero(), subject.last_evicted_id());

        subject.insert(event(1, 6));
        assert_eq!(FloEventId::new(1, 1), subject.last_evicted_id());

        subject.insert(event(1, 9));
        assert_eq!(FloEventId::new(1, 2), subject.last_evicted_id());
    }

    #[test]
    fn events_are_inserted_and_retrieved_by_range() {
        let max = 5;
        let mut subject = Cache::new(max, MemoryLimit::new(5, MemoryUnit::Megabyte), FloEventId::zero());

        let one = subject.insert(event(1, 1));
        let two = subject.insert(event(1, 2));
        let three = subject.insert(event(1, 3));
        let four = subject.insert(event(1, 4));
        let five = subject.insert(event(1, 5));

        let all_events: Vec<Arc<OwnedFloEvent>> = subject.iter(FloEventId::zero()).map(|(_, e)| e.clone()).collect();
        assert_eq!(vec![one.clone(), two.clone(), three.clone(), four.clone(), five.clone()], all_events);

        let last_two: Vec<Arc<OwnedFloEvent>> = subject.iter(FloEventId::new(1, 3)).map(|(_, e)| e.clone()).collect();
        assert_eq!(vec![four, five], last_two);
    }

    fn event(actor: ActorId, count: EventCounter) -> OwnedFloEvent {
        let id = FloEventId::new(actor, count);
        let timestamp = ::event::time::from_millis_since_epoch(5678);
        OwnedFloEvent::new(id, None, timestamp, "/foo/bar".to_owned(), vec![1, 2, 3])
    }

}
