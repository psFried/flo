use std::path::Path;
use std::collections::HashMap;
use std::io;

use event::{Event, EventId, Json};
use server::consumer::ConsumerNotifier;
use context::Consumer;
use event_store::{EventStore, PersistenceResult};



pub struct Namespace<S: EventStore, N: ConsumerNotifier> {
    pub event_store: S,
	name: String, 
    consumers: HashMap<usize, Consumer<N>>,
    current_event_id: EventId,
}

impl <S: EventStore, N: ConsumerNotifier> Namespace<S, N> {
    
    pub fn new(base_dir: &Path, namespace: String) -> Result<Namespace<S, N>, io::Error> {
        S::create(base_dir, &namespace).map(|store| {
            Namespace {
                name: namespace,
                event_store: store,
                consumers: HashMap::new(),
				current_event_id: 0,
            }
        })
    }

	pub fn add_event(&mut self, event_json: Json) -> PersistenceResult {
		let event_id = self.next_event_id();
		let event = Event::new(event_id, event_json);
		let storage_result = self.event_store.store(event);
		if storage_result.is_ok() {
		    self.notify_all_consumers();
		}
		storage_result
	}

	pub fn get_consumer(&mut self, consumer_id: usize) -> Option<&mut Consumer<N>> {
	    self.consumers.get_mut(&consumer_id)
	}

	pub fn add_consumer(&mut self, mut consumer: Consumer<N>) {
		trace!("adding consumer {} to namespace: {:?}", consumer.id, self.name);
	    notify_if_new_event(self.current_event_id, &mut consumer);
		self.consumers.insert(consumer.id, consumer);
	}

	fn notify_all_consumers(&mut self) {
		let Namespace {ref mut consumers, current_event_id, ..} = *self;
	    for (id, consumer) in consumers.iter_mut() {
	        notify_if_new_event(current_event_id, consumer);
	    }
	}

	fn next_event_id(&mut self) -> EventId {
	    self.current_event_id += 1;
	    self.current_event_id
	}

}

	#[inline]
	fn notify_if_new_event<N: ConsumerNotifier>(current_event_id: EventId, consumer: &mut Consumer<N>) {
	    if consumer.last_event < current_event_id {
	        consumer.notify();
	    }
	}

