
use std::rc::Rc;
use std::cell::RefCell;

use futures::task::{self, Task};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ConsumerStatus {
    NoChange,
    Stop,
    NextBatch,
}

const NO_CHANGE: usize = 0;
const STOP: usize = 1;
const NEXT_BATCH: usize = 2;

#[derive(Debug)]
struct Inner {
    state: ConsumerStatus,
    task: Option<Task>
}

impl Inner {
    fn new() -> Inner {
        Inner {
            state: ConsumerStatus::NoChange,
            task: None,
        }
    }
}

#[derive(Debug)]
pub struct ConsumerStatusChecker(Rc<RefCell<Inner>>);

impl ConsumerStatusChecker {
    pub fn get(&self) -> ConsumerStatus {
        let mut inner = self.0.borrow_mut();
        let val = inner.state;
        inner.state = ConsumerStatus::NoChange;
        val
    }

    pub fn await_status_change(&self) {
        let mut inner = self.0.borrow_mut();
        if inner.task.is_none() || !inner.task.as_ref().unwrap().will_notify_current() {
            inner.task = Some(task::current());
        }
    }
}

#[derive(Debug)]
pub struct ConsumerStatusSetter(Rc<RefCell<Inner>>);

impl ConsumerStatusSetter {
    pub fn set(&mut self, status: ConsumerStatus) {
        let mut inner = self.0.borrow_mut();
        inner.state = status;
        if let Some(ref task) = inner.task {
            task.notify();
        }
    }
}

pub fn create_status_channel() -> (ConsumerStatusSetter, ConsumerStatusChecker) {
    let inner = Rc::new(RefCell::new(Inner::new()));
    let setter = ConsumerStatusSetter(inner.clone());
    let checker = ConsumerStatusChecker(inner);
    (setter, checker)
}

