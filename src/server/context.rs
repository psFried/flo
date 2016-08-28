use std::collections::HashMap;
use std::path::PathBuf;
use std::io;
use serde_json::Value;

use server::namespace::Namespace;
use server::consumer::ConsumerNotifier;
use event_store::{EventStore, PersistenceResult};
use event::{EventId, Event};


const TEMP_MAX_EVENTS: usize = 100;

pub struct FloContext<N: ConsumerNotifier, S: EventStore> {
    namespaces: HashMap<String, Namespace<S, N>>,
    consumer_to_ns: HashMap<usize, String>,
    next_consumer_id: usize,
    data_dir: PathBuf,
}

pub struct Consumer<N: ConsumerNotifier> {
    pub last_event: EventId,
    pub id: usize,
    notifier: N,
}

impl<N: ConsumerNotifier> Consumer<N> {
    pub fn new(consumer_id: usize, last_event: EventId, notifier: N) -> Consumer<N> {
        Consumer {
            last_event: last_event,
            id: consumer_id,
            notifier: notifier,
        }
    }
    pub fn notify(&mut self) {
        self.notifier.notify();
    }
}

impl<N: ConsumerNotifier, S: EventStore> FloContext<N, S> {
    pub fn new(storage_dir: PathBuf) -> FloContext<N, S> {
        FloContext {
            namespaces: HashMap::new(),
            next_consumer_id: 0usize,
            consumer_to_ns: HashMap::new(),
            data_dir: storage_dir,
        }
    }

    pub fn add_event(&mut self, event_json: Value, namespace: &str) -> PersistenceResult {
        let result = self.with_namespace(namespace, |ns| ns.add_event(event_json));

        match result {
            Ok(add_result) => add_result,
            Err(io_err) => Err(io_err),
        }
    }

    pub fn add_consumer_to_namespace(&mut self, notifier: N, last_event: EventId, namespace: &str) -> usize {
        let consumer_id = self.get_next_consumer_id();
        debug!("Adding consumer: {}, namespace: \"{}\", lastEvent: {}",
               consumer_id,
               namespace,
               last_event);

        self.with_namespace(namespace, move |ns| {
                ns.add_consumer(Consumer {
                    notifier: notifier,
                    last_event: last_event,
                    id: consumer_id,
                });
            })
            .unwrap();

        self.consumer_to_ns.insert(consumer_id, namespace.to_string());
        consumer_id
    }

    fn with_namespace<T, F: FnOnce(&mut Namespace<S, N>) -> T>(&mut self, name: &str, fun: F) -> Result<T, io::Error> {
        let FloContext { ref mut namespaces, ref data_dir, .. } = *self;
        if namespaces.contains_key(name) {
            Ok(fun(namespaces.get_mut(name).unwrap()))
        } else {
            Namespace::new(data_dir, name.to_string(), TEMP_MAX_EVENTS).map(|mut ns| {
                let t = fun(&mut ns);
                namespaces.insert(name.to_string(), ns);
                t
            })
        }
    }

    pub fn get_next_event(&mut self, consumer_id: usize) -> Option<&mut Event> {
        self.get_namespace_for_consumer(consumer_id).and_then(|ns| ns.get_next_event(consumer_id))
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

    fn get_namespace_for_consumer(&mut self, consumer_id: usize) -> Option<&mut Namespace<S, N>> {
        let FloContext { ref mut consumer_to_ns, ref mut namespaces, .. } = *self;
        consumer_to_ns.get(&consumer_id).and_then(move |ns_name| namespaces.get_mut(ns_name))
    }

    fn get_consumer(&mut self, consumer_id: usize) -> Option<&mut Consumer<N>> {
        self.get_namespace_for_consumer(consumer_id).and_then(|ns| ns.get_consumer(consumer_id))
    }

    fn get_next_consumer_id(&mut self) -> usize {
        self.next_consumer_id += 1;
        self.next_consumer_id
    }
}


#[cfg(test)]
mod test {
    use test_utils::{MockConsumerNotifier, MockEventStore, create_test_flo_context};
    use event::{to_json, EventId, Event, to_event, ObjectBuilder};
    use super::*;

    #[test]
    fn get_next_event_returns_the_next_greatest_event_for_the_namespace_of_the_consumer() {
        use test_utils::create_test_namespace;

        let consumers_last_event: EventId = 0;
        let expected_event = to_event(4, r#"{"foo": "bar"}"#).unwrap();

        let mut context = create_test_flo_context();
        let namespace = "space-space-space";
        let mut ns = create_test_namespace();
        ns.event_store.stub_get_event_greater_than(consumers_last_event, expected_event.clone());
        context.namespaces.insert(namespace.to_string(), ns);

        let notifier = MockConsumerNotifier::new();
        let consumer_id = context.add_consumer_to_namespace(notifier, consumers_last_event, namespace);

        let result = context.get_next_event(consumer_id);
        assert!(result.is_some());
        assert_eq!(&expected_event, result.unwrap());
    }

    #[test]
    fn confirm_event_written_notifies_consumer_if_more_events_are_ready_to_be_written() {
        let mut context = create_test_flo_context();
        let consumer_id = context.add_consumer_to_namespace(MockConsumerNotifier::new(), 23, "theNS");

        context.confirm_event_written(consumer_id, 24);
        assert_consumer_notified(consumer_id, &mut context);
    }

    #[test]
    fn add_consumer_notifies_consumer_if_last_event_is_les_than_context_current_event_id() {
        let mut context = create_test_flo_context();
        let test_event = ObjectBuilder::new().insert("what", "evar").unwrap();
        context.add_event(test_event, "theNS").unwrap();
        let consumer_id = context.add_consumer_to_namespace(MockConsumerNotifier::new(), 0, "theNS");
        assert_consumer_notified(consumer_id, &mut context);
    }

    #[test]
    fn last_event_id_does_not_change_if_new_event_id_is_less_than_the_previous_one() {
        let mut context = create_test_flo_context();

        let starting_event_id = 44;
        let consumer_id = context.add_consumer_to_namespace(MockConsumerNotifier::new(), starting_event_id, "theNS");
        assert_eq!(starting_event_id,
                   context.get_consumer(consumer_id).unwrap().last_event);

        context.confirm_event_written(consumer_id, 43);
        assert_eq!(starting_event_id,
                   context.get_consumer(consumer_id).unwrap().last_event);
    }

    #[test]
    fn confirm_event_written_sets_a_consumers_latest_event_id() {
        let mut context = create_test_flo_context();

        let consumer_id = context.add_consumer_to_namespace(MockConsumerNotifier::new(), 0, "theNS");
        assert_eq!(0, context.get_consumer(consumer_id).unwrap().last_event);

        let confirmed_event: EventId = 67;
        context.confirm_event_written(consumer_id, confirmed_event);
        assert_eq!(confirmed_event,
                   context.get_consumer(consumer_id).unwrap().last_event);
    }

    #[test]
    fn add_event_adds_events_to_the_correct_namespace() {
        let mut context = create_test_flo_context();

        let namespace = "theNamespace";
        let evt1 = to_json(r#"{"someKey": "someValue1"}"#).unwrap();
        context.add_event(evt1.clone(), "theNamespace").unwrap();

        context.with_namespace(namespace, |ns| {
                   let expected_event = Event::new(1, evt1);
                   ns.event_store.assert_event_was_stored(&expected_event);
               })
               .unwrap();
    }

    #[test]
    fn get_next_event_returns_none_if_event_store_is_empty() {
        let mut context = create_test_flo_context();

        let notifier = MockConsumerNotifier::new();
        let consumer_id = context.add_consumer_to_namespace(notifier, 0, "any-old-namespace");
        let result = context.get_next_event(consumer_id);
        assert!(result.is_none());
    }

    #[test]
    fn add_consumer_creates_consumer_with_the_given_last_event_id() {
        let mut ctx = create_test_flo_context();
        let event_id: EventId = 88;

        let consumer_id = ctx.add_consumer_to_namespace(MockConsumerNotifier::new(), event_id, "anyNamespace");
        let consumer = ctx.get_consumer(consumer_id).unwrap();
        assert_eq!(event_id, consumer.last_event);
    }

    #[test]
    fn add_consumer_returns_the_id_of_the_consumer_created() {
        let mut ctx = create_test_flo_context();

        let namespace = "dafuqisdat";
        let result = ctx.add_consumer_to_namespace(MockConsumerNotifier::new(), 0, namespace);
        assert_eq!(1, result);

        let result = ctx.add_consumer_to_namespace(MockConsumerNotifier::new(), 0, namespace);
        assert_eq!(2, result);

    }

    fn assert_consumer_notified(consumer_id: usize, context: &mut FloContext<MockConsumerNotifier, MockEventStore>) {
        context.get_consumer(consumer_id).unwrap().notifier.assert_notify_was_called();
    }
}
