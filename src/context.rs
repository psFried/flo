
use rotor::Notifier;

pub struct FloContext {
    events: Vec<String>,
    consumers: Vec<Notifier>,
}

impl FloContext {

    pub fn new() -> FloContext {
        FloContext {
            events: Vec::new(),
            consumers: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: String) {
        self.events.push(event);
    }

    pub fn add_consumer(&mut self, consumer_notifier: Notifier) {
        self.consumers.push(consumer_notifier);
    }

    pub fn notify_all_consumers(&self) {
        for consumer in self.consumers.iter() {
            consumer.wakeup().unwrap();
        }
    }

    pub fn last_event(&self) -> Option<&String> {
        self.events.last()
    }
}
