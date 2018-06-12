

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use event::EventCounter;


#[derive(Clone, Debug)]
pub struct HighestCounter(Arc<AtomicUsize>);

impl HighestCounter {
    pub fn zero() -> HighestCounter {
        HighestCounter::starting_at(0)
    }

    pub fn starting_at(count: EventCounter) -> HighestCounter {
        HighestCounter(Arc::new(AtomicUsize::new(count as usize)))
    }

    pub fn set_if_greater(&self, value: EventCounter) {
        let mut current = self.0.load(Ordering::SeqCst);
        let mut attempts = 1;
        loop {
            if value > current as EventCounter {
                let prev = self.0.compare_and_swap(current, value as usize, Ordering::SeqCst);
                if prev == current {
                    break;
                } else {
                    current = prev;
                    attempts += 1;
                }

            } else {
                break;
            }
        }

        // I have no idea if this implementation is any good at all, so maybe let's log some things and find out
        if attempts > 1 {
            debug!("set_if_greater took: {} attempts", attempts);
        }
    }

    pub fn increment_and_get(&self, inc_amount: EventCounter) -> EventCounter {
        let mut current = self.0.load(Ordering::SeqCst);
        let mut attempts = 1;
        loop {
            let new_value = current + inc_amount as usize;
            let prev = self.0.compare_and_swap(current, new_value, Ordering::SeqCst);
            if prev == current {
                break; //success !
            } else {
                current = prev;
                attempts += 1;
            }
        }

        // I have no idea if this implementation is any good at all, so maybe let's log some things and find out
        if attempts > 1 {
            debug!("set_if_greater took: {} attempts", attempts);
        }
        current as EventCounter + inc_amount
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::SeqCst) as u64
    }

}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn set_if_greater_sets_value_when_new_value_is_greater() {
        let subject = HighestCounter::zero();
        assert_eq!(0, subject.get());
        subject.set_if_greater(9);
        assert_eq!(9, subject.get());
        subject.set_if_greater(8);
        assert_eq!(9, subject.get());
    }

    #[test]
    fn increment_and_get_adds_to_current_value() {
        let subject = HighestCounter::zero();

        let new = subject.increment_and_get(6);
        assert_eq!(6, new);
        assert_eq!(6, subject.get());

        let new = subject.increment_and_get(7);
        assert_eq!(13, new);
        assert_eq!(13, subject.get());
    }

}

