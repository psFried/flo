
use consumer::FloConsumer;
use rotor::Notifier;
use serde_json::Value;
use event_store::{EventStore, PersistenceResult};


pub struct FloContext<C: FloConsumer, S: EventStore> {
    pub events: Vec<Value>,
    pub consumers: Vec<C>,
    pub event_store: S,
}

impl <C: FloConsumer, S: EventStore> FloContext<C, S> {

    pub fn new(event_store: S) -> FloContext<C, S> {
        FloContext {
            events: Vec::new(),
            consumers: Vec::new(),
            event_store: event_store,
        }
    }

    pub fn add_event(&mut self, event: Value) -> PersistenceResult {
        self.events.push(event);
        Ok(())
    }

    pub fn add_consumer(&mut self, consumer: C) {
        self.consumers.push(consumer);
    }

    pub fn notify_all_consumers(&mut self) {
        for consumer in self.consumers.iter_mut() {
            consumer.notify();
        }
    }

    pub fn last_event(&self) -> Option<&Value> {
        self.events.last()
    }
}
