
use server::consumer::ConsumerNotifier;
use serde_json::Value;
use event_store::{EventStore, PersistenceResult};
use event::{EventId, Event, Json};
use std::collections::HashMap;
use std::cmp::max;


pub struct FloContext<N: ConsumerNotifier, S: EventStore> {
    pub event_store: S,
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
    fn notify(&mut self) {
        self.notifier.notify();
    }
}

impl <N: ConsumerNotifier, S: EventStore> FloContext<N, S> {

    pub fn new(event_store: S) -> FloContext<N, S> {
        FloContext {
            consumers: HashMap::new(),
            event_store: event_store,
            next_consumer_id: 0usize,
            current_event_id: 0,
        }
    }

    pub fn add_event(&mut self, event_json: Value) -> PersistenceResult {
        let event_id = self.next_event_id();
        let event = Event::new(event_id, event_json);
        self.event_store.store(event);
        self.notify_all_consumers();
        Ok(())
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

    fn next_event_id(&mut self) -> EventId {
        self.current_event_id += 1;
        self.current_event_id
    }

    fn get_consumer(&mut self, consumer_id: usize) -> Option<&mut Consumer<N>> {
        self.consumers.get_mut(&consumer_id)
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
    use test_utils::{MockConsumerNotifier, MockEventStore, create_test_flo_context};
    use event::{to_json, EventId, Event, to_event};
    use tempdir::TempDir;
    use super::*;

    #[test]
    fn confirm_event_written_notifies_consumer_if_more_events_are_ready_to_be_written() {
        let mut context = create_test_flo_context();
        let consumer_id = context.add_consumer(MockConsumerNotifier::new(), 23);
        context.current_event_id = 25;
        context.confirm_event_written(consumer_id, 24);
        assert_consumer_notified(consumer_id, &mut context);
    }

    #[test]
    fn add_consumer_notifies_consumer_if_last_event_is_les_than_context_current_event_id() {
        let mut context = create_test_flo_context();
        context.current_event_id = 55;
        let consumer_id = context.add_consumer(MockConsumerNotifier::new(), 23);
        assert_consumer_notified(consumer_id, &mut context);
    }

    #[test]
    fn confirm_event_written_does_not_change_last_event_id_if_new_event_id_is_less_than_the_previous_one() {
        let mut context = create_test_flo_context();

        let starting_event_id = 44;
        let consumer_id = context.add_consumer(MockConsumerNotifier::new(), starting_event_id);
        assert_eq!(starting_event_id, context.get_consumer(consumer_id).unwrap().last_event);

        context.confirm_event_written(consumer_id, 43);
        assert_eq!(starting_event_id, context.get_consumer(consumer_id).unwrap().last_event);
    }

    #[test]
    fn confirm_event_written_sets_a_consumers_latest_event_id() {
        let mut context = create_test_flo_context();

        let consumer_id = context.add_consumer(MockConsumerNotifier::new(), 0);
        assert_eq!(0, context.get_consumer(consumer_id).unwrap().last_event);

        let confirmed_event: EventId = 67;
        context.confirm_event_written(consumer_id, confirmed_event);
        assert_eq!(confirmed_event, context.get_consumer(consumer_id).unwrap().last_event);
    }

    #[test]
    fn add_event_creates_an_event_with_the_next_sequential_event_id() {
        let mut context = create_test_flo_context();

        let evt1 = to_json(r#"{"someKey": "someValue1"}"#).unwrap();
        context.add_event(evt1.clone());

        let expected_event1 = Event::new(1, evt1);
        context.event_store.assert_event_was_stored(&expected_event1);

        let evt2 = to_json(r#"{"aDifferentKey": "totallyIrrelevantValue"}"#).unwrap();
        context.add_event(evt2.clone());

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
