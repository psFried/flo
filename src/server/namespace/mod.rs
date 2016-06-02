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
		debug!("adding event to namespace: {}, event: {:?}", self.name, event);
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

	pub fn get_next_event(&mut self, consumer_id: usize) -> Option<&mut Event> {
       match self.consumers.get(&consumer_id) {
            Some(consumer) => self.event_store.get_event_greater_than(consumer.last_event),
            _ => None
        }
 
	}

	fn notify_all_consumers(&mut self) {
		let Namespace {ref mut consumers, current_event_id, ..} = *self;
	    for (_id, consumer) in consumers.iter_mut() {
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


#[cfg(test)]
mod test {
	use test_utils::{MockConsumerNotifier, create_test_namespace};
	use event::{Event, ObjectBuilder};
	use context::Consumer;
    
    #[test]
    fn get_next_event_returns_next_event_for_the_consumer() {
        let mut subject = create_test_namespace();
		let consumer_last_event = 13;
		let consumer_id = 0usize;
        
        let event_json = ObjectBuilder::new().insert("spellll", "chuck").unwrap();
		for _ in 0..15 {
		    subject.add_event(event_json.clone()).unwrap();
		}
		subject.add_consumer(Consumer::new(consumer_id, consumer_last_event , MockConsumerNotifier::new()));
		let mut expected_event = Event::new(consumer_last_event + 1, event_json.clone());

		subject.event_store.stub_get_event_greater_than(consumer_last_event, expected_event.clone());
		let result = subject.get_next_event(consumer_id);
		assert_eq!(Some(&mut expected_event), result);
    }
    
}
