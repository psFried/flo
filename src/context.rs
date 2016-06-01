use std::collections::HashMap;
use std::path::PathBuf;
use std::io;
use serde_json::Value;

use server::namespace::Namespace;
use server::consumer::ConsumerNotifier;
use event_store::{EventStore, PersistenceResult};
use event::{EventId, Event};


pub struct FloContext<N: ConsumerNotifier, S: EventStore> {
    pub event_store: S,
	namespaces: HashMap<String, Namespace<S, N>>,
	consumer_to_ns: HashMap<usize, String>,
    consumers: HashMap<usize, Consumer<N>>,
    next_consumer_id: usize,
    current_event_id: EventId,
}

pub struct Consumer<N: ConsumerNotifier> {
    pub last_event: EventId,
    pub id: usize,
    notifier: N,
}

impl <N: ConsumerNotifier> Consumer<N> {
    pub fn notify(&mut self) {
        self.notifier.notify();
    }
}

impl <N: ConsumerNotifier, S: EventStore> FloContext<N, S> {

    pub fn new(event_store: S) -> FloContext<N, S> {
        FloContext {
            consumers: HashMap::new(),
			namespaces: HashMap::new(),
            event_store: event_store,
            next_consumer_id: 0usize,
            current_event_id: 0,
			consumer_to_ns: HashMap::new(),
        }
    }

    pub fn add_event(&mut self, event_json: Value, namespace: &str) -> PersistenceResult {
		if self.namespaces.contains_key(namespace) {
		    let ref mut ns = self.namespaces.get_mut(namespace).unwrap();
		    ns.add_event(event_json)
		} else {
			let path = PathBuf::from(".");
		    Namespace::new(&path, namespace.to_string()).and_then(|mut ns| {
                let result = ns.add_event(event_json);
                self.namespaces.insert(namespace.to_string(), ns);
                result
            })
		}
    }

	pub fn add_consumer_to_namespace(&mut self, notifier: N, last_event: EventId, namespace: &str) -> usize {
        let consumer_id = self.get_next_consumer_id();
        debug!("Adding consumer: {}, lastEvent: {}", consumer_id, last_event);
	    
		self.with_namespace(namespace, move |ns| {
            ns.add_consumer(Consumer {
                notifier: notifier,
                last_event: last_event,
                id: consumer_id,
            });
        });

		self.consumer_to_ns.insert(consumer_id, namespace.to_string());
		consumer_id
	}

	fn with_namespace<T, F: FnOnce(&mut Namespace<S, N>) -> T>(&mut self, name: &str, fun: F) -> Result<T, io::Error> {
	    if self.namespaces.contains_key(name) {
	        Ok(fun(self.namespaces.get_mut(name).unwrap()))
	    } else {
			let path = PathBuf::from(".");
	        Namespace::new(&path, name.to_string()).map(|mut ns| {
                let t = fun(&mut ns);
                self.namespaces.insert(name.to_string(), ns);
                t
            })
	    }
	}

    pub fn add_consumer(&mut self, notifier: N, last_event: EventId) -> usize {
        let consumer_id = self.get_next_consumer_id();
        debug!("Adding consumer: {}, lastEvent: {}", consumer_id, last_event);
        let mut consumer = Consumer {
            notifier: notifier,
            last_event: last_event,
            id: consumer_id,
        };
        if last_event < self.current_event_id {
            consumer.notify();
        }

        self.consumers.insert(consumer_id, consumer);
        consumer_id
    }

    pub fn get_next_event(&mut self, consumer_id: usize) -> Option<&mut Event> {
        match self.consumers.get(&consumer_id) {
            Some(consumer) => self.event_store.get_event_greater_than(consumer.last_event),
            _ => None
        }
    }

    pub fn confirm_event_written(&mut self, consumer_id: usize, event_id: EventId) {
        self.get_consumer(consumer_id).map(|consumer| {
            if event_id > consumer.last_event {
                trace!("consumer: {}, confirmed event: {}", consumer_id, event_id);
                consumer.last_event = event_id;
                consumer.notify();
            }
        });
    }


    fn get_consumer(&mut self, consumer_id: usize) -> Option<&mut Consumer<N>> {
		let FloContext{ref mut consumer_to_ns, ref mut namespaces, .. } = *self;
		consumer_to_ns.get(&consumer_id).and_then(move |ns_name| {
            namespaces.get_mut(ns_name)
        }).and_then(|ns| {
            ns.get_consumer(consumer_id)
        })
    }

    fn get_next_consumer_id(&mut self) -> usize {
        self.next_consumer_id += 1;
        self.next_consumer_id
    }

}


#[cfg(test)]
mod test {
    use test_utils::{MockConsumerNotifier, MockEventStore, create_test_flo_context};
    use event::{to_json, EventId, Event, to_event};
    use super::*;

    #[test]
    fn confirm_event_written_notifies_consumer_if_more_events_are_ready_to_be_written() {
        let mut context = create_test_flo_context();
        let consumer_id = context.add_consumer_to_namespace(MockConsumerNotifier::new(), 23, "theNS");
        context.current_event_id = 25;
        context.confirm_event_written(consumer_id, 24);
        assert_consumer_notified(consumer_id, &mut context);
    }

    #[test]
    fn add_consumer_notifies_consumer_if_last_event_is_les_than_context_current_event_id() {
        let mut context = create_test_flo_context();
        context.current_event_id = 55;
        let consumer_id = context.add_consumer_to_namespace(MockConsumerNotifier::new(), 23, "theNS");
        assert_consumer_notified(consumer_id, &mut context);
    }

    #[test]
    fn confirm_event_written_does_not_change_last_event_id_if_new_event_id_is_less_than_the_previous_one() {
        let mut context = create_test_flo_context();

        let starting_event_id = 44;
        let consumer_id = context.add_consumer_to_namespace(MockConsumerNotifier::new(), starting_event_id, "theNS");
        assert_eq!(starting_event_id, context.get_consumer(consumer_id).unwrap().last_event);

        context.confirm_event_written(consumer_id, 43);
        assert_eq!(starting_event_id, context.get_consumer(consumer_id).unwrap().last_event);
    }

    #[test]
    fn confirm_event_written_sets_a_consumers_latest_event_id() {
        let mut context = create_test_flo_context();

        let consumer_id = context.add_consumer_to_namespace(MockConsumerNotifier::new(), 0, "theNS");
        assert_eq!(0, context.get_consumer(consumer_id).unwrap().last_event);

        let confirmed_event: EventId = 67;
        context.confirm_event_written(consumer_id, confirmed_event);
        assert_eq!(confirmed_event, context.get_consumer(consumer_id).unwrap().last_event);
    }

    #[test]
    fn add_event_creates_an_event_with_the_next_sequential_event_id() {
        let mut context = create_test_flo_context();

        let evt1 = to_json(r#"{"someKey": "someValue1"}"#).unwrap();
        context.add_event(evt1.clone(), "theNamespace").unwrap();

        let expected_event1 = Event::new(1, evt1);
        context.event_store.assert_event_was_stored(&expected_event1);

        let evt2 = to_json(r#"{"aDifferentKey": "totallyIrrelevantValue"}"#).unwrap();
        context.add_event(evt2.clone(), "theNamespace").unwrap();

		//TODO: assert event was added to the correct namespace

        let expected_event2 = Event::new(2, evt2);
        context.event_store.assert_event_was_stored(&expected_event2);
    }

    #[test]
    fn get_next_event_delegates_to_event_store() {
        let mut context = create_test_flo_context();

        let consumers_last_event: EventId = 0;
        let expected_event = to_event(4, r#"{"foo": "bar"}"#).unwrap();
        context.event_store.stub_get_event_greater_than(consumers_last_event, expected_event.clone());

        let notifier = MockConsumerNotifier::new();
        let consumer_id = context.add_consumer(notifier, consumers_last_event);

        let result = context.get_next_event(consumer_id);
        assert!(result.is_some());
        assert_eq!(&expected_event, result.unwrap());
    }

    #[test]
    fn get_next_event_returns_none_if_event_store_is_empty() {
        let mut context = create_test_flo_context();

        let notifier = MockConsumerNotifier::new();
        let consumer_id = context.add_consumer(notifier, 0);
        let result = context.get_next_event(consumer_id);
        assert!(result.is_none());
    }

    #[test]
    fn add_consumer_creates_consumer_with_the_given_last_event_id() {
        let mut ctx = create_test_flo_context();
        let event_id: EventId = 88;

        let consumer_id = ctx.add_consumer(MockConsumerNotifier::new(), event_id);
        let consumer = ctx.get_consumer(consumer_id).unwrap();
        assert_eq!(event_id, consumer.last_event);
    }

    #[test]
    fn add_consumer_returns_the_id_of_the_consumer_created() {
        let mut ctx = create_test_flo_context();
        let result = ctx.add_consumer(MockConsumerNotifier::new(), 0);
        assert_eq!(1, result);

        let result = ctx.add_consumer(MockConsumerNotifier::new(), 0);
        assert_eq!(2, result);

    }

    fn assert_consumer_notified(consumer_id: usize, context: &mut FloContext<MockConsumerNotifier, MockEventStore>) {
        context.get_consumer(consumer_id).unwrap().notifier.assert_notify_was_called();
    }
}
