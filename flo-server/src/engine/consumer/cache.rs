use std::sync::Arc;
use std::collections::{BTreeMap, Bound};
use std::collections::btree_map::Range;

use chrono::Duration;
use event::{FloEventId, FloEvent, OwnedFloEvent, Timestamp, time};
use server::MemoryLimit;

struct EntryInfo {
    id: FloEventId,
    timestamp: Timestamp,
}

pub struct Cache {
    entries: BTreeMap<FloEventId, Arc<OwnedFloEvent>>,
    first: Option<EntryInfo>,
    last: Option<EntryInfo>,
    last_evicted_id: FloEventId,
    expiration_time: Duration,
    max_memory: usize,
    current_memory: usize,
}

impl Cache {

    /// Ok, this is a little weird since caching is handled at higher level than storage...
    /// When we initialize the cache, we need to know if there are pre-existing events that
    /// aren't cached. This is the purpose of the `greatest_uncached_event` parameter.
    pub fn new(expiration_time: Duration, max_memory: MemoryLimit, greatest_uncached_event: FloEventId) -> Cache {
        info!("Initializing event cache with expiration_time: {}, max_memory: {:?}, greatest_uncached_event: {}", expiration_time, max_memory, greatest_uncached_event);
        Cache {
            entries: BTreeMap::new(),
            first: None,
            last: None,
            last_evicted_id: greatest_uncached_event,
            expiration_time: expiration_time,
            max_memory: max_memory.as_bytes(),
            current_memory: 0,
        }
    }

    pub fn iter(&self, start_exclusive: FloEventId) -> Range<FloEventId, Arc<OwnedFloEvent>> {
        self.entries.range((Bound::Excluded(&start_exclusive), Bound::Unbounded))
    }

    pub fn insert(&mut self, event: OwnedFloEvent) -> Arc<OwnedFloEvent> {
        let event_id = event.id;
        if self.first.is_none() {
            self.first = Some(EntryInfo{
                id: event_id,
                timestamp: event.timestamp,
            });
        }

        if self.last.as_ref().map(|info| info.id < event_id).unwrap_or(true) {
            self.last = Some(EntryInfo{
                id: event_id,
                timestamp: event.timestamp,
            });
        }

        let event_size = size_of(&event);

        self.remove_expired_events();

        while self.current_memory + event_size > self.max_memory {
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

    pub fn remove_expired_events(&mut self) {
        if self.entries.is_empty() {
            return;
        }

        let threshold = time::now() - self.expiration_time;
        let split_point = self.entries.iter().filter(|&(_, event)| {
            event.timestamp > threshold
        }).map(|(id, _)| *id).next();

        let first = self.first.as_ref().map(|f| f.id).unwrap_or(FloEventId::zero());

        if let Some(split_id) = split_point {
            // if the split_id is <= first, then we shouldn't remove any entries
            if split_id > first {
                // we're removing at least one entry
                let mut new_entries = self.entries.split_off(&split_id);
                debug!("evicting {} expired entries from cache from before threshold: {}", self.entries.len(), self.expiration_time);
                let new_last_evicted = self.entries.keys().cloned().next().unwrap_or(FloEventId::new(split_id.actor, split_id.event_counter - 1));
                self.last_evicted_id = new_last_evicted;
                ::std::mem::swap(&mut new_entries, &mut self.entries);

            }
        } else {
            // we couldn't find an event with a timestamp greater than the max, so we need to clear them all out
            debug!("Clearing all {} events from cache due to expiration threshold: {}", self.entries.len(), threshold);
            let new_last_evicted = self.entries.keys().cloned().next_back().unwrap();
            self.last_evicted_id = new_last_evicted;
            self.first = None;
            self.last = None;
            self.entries.clear();
        }
    }


    fn remove_oldest_entry(&mut self) {
        let (to_remove, new_first) = {
            let mut entries = self.entries.values();
            let remove = entries.next().map(|e| e.id);
            let first = entries.next().map(|e| {
                EntryInfo{
                    id: e.id,
                    timestamp: e.timestamp,
                }
            });
            (remove, first)
        };

        if let Some(id) = to_remove {
            self.entries.remove(&id).map(|event| {
                self.current_memory = self.current_memory.saturating_sub(size_of(&*event));
                trace!("Cache evicted event: {}, new memory_usage: {} bytes", id, self.current_memory);
                self.first = new_first;
                self.last_evicted_id = event.id;
            });
        }
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
    use std::thread;
    use super::*;
    use event::{FloEventId, ActorId, EventCounter, OwnedFloEvent, time};
    use server::{MemoryLimit, MemoryUnit};
    use chrono::Duration;


    #[test]
    fn cache_evicts_events_after_an_expiration_time() {
        let cache_expiration_duration = Duration::milliseconds(30);
        let mut subject = Cache::new(cache_expiration_duration, MemoryLimit::new(5, MemoryUnit::Megabyte), FloEventId::zero());

        fn event(counter: EventCounter) -> OwnedFloEvent {
            OwnedFloEvent::new(FloEventId::new(1, counter), None, time::now(), "/foo".to_owned(), Vec::new())
        }

        for i in 0..20 {
            subject.insert(event(i + 1));
            thread::sleep(::std::time::Duration::from_millis(1));
        }

        let full_expiration = time::now() + cache_expiration_duration + Duration::milliseconds(100);

        let mut event_count = 20;
        let mut min = FloEventId::zero();

        while time::now() < full_expiration && event_count > 0 {
            subject.remove_expired_events();

            let count = subject.iter(FloEventId::zero()).count();
            assert!(count <= event_count);
            event_count = count;

            let first_id = subject.iter(FloEventId::zero()).map(|(id, _)| *id).next();
            if first_id.is_none() {
                break;
            }
            let first_id = first_id.unwrap();
            assert!(first_id >= min);
            min = first_id;

            thread::sleep(::std::time::Duration::from_millis(1));
        }

        assert_eq!(0, event_count);
    }

    #[test]
    fn events_are_inserted_and_retrieved_by_range() {
        let mut subject = Cache::new(Duration::days(5), MemoryLimit::new(5, MemoryUnit::Megabyte), FloEventId::zero());

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
        OwnedFloEvent::new(id, None, time::now(), "/foo/bar".to_owned(), vec![1, 2, 3])
    }

}
