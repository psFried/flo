
use engine::ConnectionId;
use engine::event_stream::partition::ConsumerNotifier;

pub struct ConsumerManager {
    uncommitted: Vec<Box<ConsumerNotifier>>,
    committed: Vec<Box<ConsumerNotifier>>,
}

impl ConsumerManager {

    pub fn new() -> ConsumerManager {
        ConsumerManager {
            uncommitted: Vec::with_capacity(4),
            committed: Vec::with_capacity(4)
        }
    }

    pub fn add_committed(&mut self, consumer: Box<ConsumerNotifier>) {
        self.committed.push(consumer);
    }

    pub fn add_uncommitted(&mut self, consumer: Box<ConsumerNotifier>) {
        self.uncommitted.push(consumer);
    }

    pub fn remove(&mut self, consumer: ConnectionId) {
        self.uncommitted.retain(|notifier| {
            notifier.connection_id() != consumer
        })
    }

    pub fn notify_committed(&mut self) {
        // consumers of uncommitted events should also get notified so that they can send out an update about the commit index
        self.notify_uncommitted();
        do_notify("committed", &mut self.committed);
    }

    pub fn notify_uncommitted(&mut self) {
        do_notify("uncommitted", &mut self.uncommitted);
    }
}

fn do_notify(consumer_type: &str, consumers: &mut Vec<Box<ConsumerNotifier>>) {
    let mut count = 0;
    consumers.retain(|consumer| {
        let active = consumer.is_active();
        if active {
            consumer.notify();
            count += 1;
        } else {
            debug!("Removing consumer for connection_id: {} because it is inactive", consumer.connection_id());
        }
        active
    });
    debug!("Notified {} {} consumers", count, consumer_type);
}
