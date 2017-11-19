//! shared data structures used to communicate between the Consumer and a backend Partition

use std::sync::Arc;

use futures::task::AtomicTask;

use engine::ConnectionId;
use engine::event_stream::partition::ConsumerNotifier;
use atomics::{AtomicBoolReader, AtomicBoolWriter};


pub struct ConsumerNotifierImpl {
    task_ref: Arc<AtomicTask>,
    active: AtomicBoolReader,
    connection_id: ConnectionId,
}

impl ConsumerNotifier for ConsumerNotifierImpl {
    fn notify(&self) {
        self.task_ref.notify();
    }

    fn is_active(&self) -> bool {
        self.active.get_relaxed()
    }

    fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }
}


#[derive(Debug)]
pub struct ConsumerTaskSetter {
    task_ref: Arc<AtomicTask>,
    active: AtomicBoolWriter,
}

impl ConsumerTaskSetter {

    pub fn create() -> ConsumerTaskSetter {
        ConsumerTaskSetter {
            task_ref: Arc::new(AtomicTask::new()),
            active: AtomicBoolWriter::with_value(true),
        }
    }

    pub fn create_notifier(&self, connection_id: ConnectionId) -> Box<ConsumerNotifier> {
        let task_ref = self.task_ref.clone();
        let active = self.active.reader();

        Box::new(ConsumerNotifierImpl {
            task_ref,
            active,
            connection_id,
        })
    }

    #[allow(dead_code)] //TODO: implement stopping consumer
    pub fn cancel(&mut self) {
        self.active.set(false);
    }

    pub fn await_more_events(&self) {
        self.task_ref.register();
    }
}
