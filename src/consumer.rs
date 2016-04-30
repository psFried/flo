
use rotor::Notifier;


pub trait FloConsumer: Sized {

    fn notify(&mut self);

}


pub struct FloRotorConsumer {
    notifier: Notifier,
}

impl FloRotorConsumer {
    pub fn new(notifier: Notifier) -> FloRotorConsumer {
        FloRotorConsumer {
            notifier: notifier,
        }
    }
}

impl FloConsumer for FloRotorConsumer {

    fn notify(&mut self) {
        self.notifier.wakeup().unwrap();
    }
}
