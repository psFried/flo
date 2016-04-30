
use consumer::FloConsumer;
use rotor::Notifier;

pub struct FloContext<C: FloConsumer> {
    pub events: Vec<String>,
    pub consumers: Vec<C>,
}

impl <C: FloConsumer> FloContext<C> {

    pub fn new() -> FloContext<C> {
        FloContext {
            events: Vec::new(),
            consumers: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: String) {
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

    pub fn last_event(&self) -> Option<&String> {
        self.events.last()
    }
}
