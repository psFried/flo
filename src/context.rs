
use consumer::FloConsumer;
use rotor::Notifier;
use serde_json::Value;

pub struct FloContext<C: FloConsumer> {
    pub events: Vec<Value>,
    pub consumers: Vec<C>,
}

impl <C: FloConsumer> FloContext<C> {

    pub fn new() -> FloContext<C> {
        FloContext {
            events: Vec::new(),
            consumers: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: Value) {
        self.events.push(event);
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
