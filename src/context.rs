
use server::consumer::ConsumerNotifier;
use serde_json::Value;
use event_store::{EventStore, PersistenceResult};
use event::{EventId, Event, Json};
use std::collections::HashMap;


pub struct FloContext<N: ConsumerNotifier, S: EventStore> {
    pub event_store: S,
    pub events: Vec<Json>,
    consumers: HashMap<usize, Consumer<N>>,
    next_consumer_id: usize,
}

pub struct Consumer<N: ConsumerNotifier> {
    pub last_event: EventId,
    pub id: usize,
    notifier: N,
}

impl <N: ConsumerNotifier> Consumer<N> {
    fn notify(&mut self) {
        self.notifier.notify();
    }
}

impl <N: ConsumerNotifier, S: EventStore> FloContext<N, S> {

    pub fn new(event_store: S) -> FloContext<N, S> {
        FloContext {
            events: Vec::new(),
            consumers: HashMap::new(),
            event_store: event_store,
            next_consumer_id: 0usize,
        }
    }

    pub fn add_event(&mut self, event: Value) -> PersistenceResult {
        self.events.push(event);
        self.notify_all_consumers();
        Ok(())
    }

    pub fn add_consumer(&mut self, notifier: N, last_event: EventId) -> usize {
        let consumer_id = self.get_next_consumer_id();
        self.consumers.insert(consumer_id, Consumer {
            notifier: notifier,
            last_event: last_event,
            id: consumer_id,
        });
        consumer_id
    }

    pub fn get_consumer(&mut self, consumer_id: usize) -> Option<&mut Consumer<N>> {
        self.consumers.get_mut(&consumer_id)
    }

    pub fn get_next_event(&mut self, consumer_id: usize) -> Option<&Event> {
        match self.consumers.get(&consumer_id) {
            Some(consumer) => self.event_store.get_event_greater_than(consumer.last_event),
            _ => None
        }
    }

    pub fn last_event(&self) -> Option<&Json> {
        self.events.last()
    }

    fn get_next_consumer_id(&mut self) -> usize {
        self.next_consumer_id += 1;
        self.next_consumer_id
    }

    fn notify_all_consumers(&mut self) {
        for (_id, consumer) in self.consumers.iter_mut() {
            consumer.notify();
        }
    }
}

#[cfg(test)]
mod test {
    use test_utils::{MockConsumerNotifier, create_test_flo_context};
    use event::{to_json, EventId, to_event};
    use tempdir::TempDir;

    #[test]
    fn get_next_event_delegates_to_event_store() {
        let tmp_dir = TempDir::new("context-test").unwrap();
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
}
