//! shared data structures used to communicate between the Consumer and a backend Partition

use std::sync::Arc;

use futures::task::AtomicTask;

use new_engine::event_stream::partition::ConsumerNotifier;
use atomics::{AtomicBoolReader, AtomicBoolWriter};


pub struct ConsumerNotifierImpl {
    task_ref: Arc<AtomicTask>,
    active: AtomicBoolReader,
}

/// Creates a pending consumer and a notifier pair
pub fn create_consumer_notifier() -> (ConsumerTaskSetter, Box<ConsumerNotifier>) {
    let task = Arc::new(AtomicTask::new());
    let active_writer = AtomicBoolWriter::with_value(true);

    let notifier = ConsumerNotifierImpl {
        task_ref: task.clone(),
        active: active_writer.reader()
    };
    let task_setter = ConsumerTaskSetter {
        task_ref: task,
        active: active_writer,
    };

    (task_setter, Box::new(notifier) as Box<ConsumerNotifier>)
}


impl ConsumerNotifier for ConsumerNotifierImpl {
    fn notify(&mut self) {
        self.task_ref.notify();
    }

    fn is_active(&self) -> bool {
        self.active.get_relaxed()
    }
}


#[derive(Debug)]
pub struct ConsumerTaskSetter {
    task_ref: Arc<AtomicTask>,
    active: AtomicBoolWriter,
}

impl ConsumerTaskSetter {

    pub fn cancel(&mut self) {
        self.active.set(false);
    }

    pub fn await_more_events(&self) {
        self.task_ref.register();
    }
}
