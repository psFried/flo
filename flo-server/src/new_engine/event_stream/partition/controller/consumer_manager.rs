
use new_engine::ConnectionId;
use new_engine::event_stream::partition::ConsumerNotifier;

pub struct ConsumerManager {
    uncommitted_consumers: Vec<Box<ConsumerNotifier>>,
}

impl ConsumerManager {

    pub fn new() -> ConsumerManager {
        ConsumerManager {
            uncommitted_consumers: Vec::with_capacity(4)
        }
    }

    pub fn add_uncommitted(&mut self, consumer: Box<ConsumerNotifier>) {
        self.uncommitted_consumers.push(consumer);
    }

    pub fn remove(&mut self, consumer: ConnectionId) {
        self.uncommitted_consumers.retain(|notifier| {
            notifier.connection_id() != consumer
        })
    }

    pub fn notify_uncommitted(&mut self) {
        let mut count = 0;
        self.uncommitted_consumers.retain(|consumer| {
            let active = consumer.is_active();
            if active {
                consumer.notify();
                count += 1;
            } else {
                debug!("Removing consumer for connection_id: {} because it is inactive", consumer.connection_id());
            }
            active
        });
        debug!("Notified {} uncommitted consumers", count);
    }
}
