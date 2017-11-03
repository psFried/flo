//! An abstraction over an `AtomicUsize` to provide a monotonically increasing counter with one writer and many readers.
//! The value of the counter can only ever increase, and can only be mutated by a singe reference. There can be many readers,
//! though. Limiting the mutation of the value to a single reference makes certain operations easier and faster.

use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct AtomicCounterWriter {
    inner: Arc<AtomicUsize>
}

impl AtomicCounterWriter {

    pub fn zero() -> AtomicCounterWriter {
        AtomicCounterWriter::with_value(0)
    }

    pub fn with_value(value: usize) -> AtomicCounterWriter {
        AtomicCounterWriter {
            inner: Arc::new(AtomicUsize::new(value))
        }
    }

    /// increments the value by the specified amount and returns the _new_ value.
    pub fn increment_and_get_relaxed(&mut self, amount: usize) -> usize {
        let old = self.fetch_add(amount, Ordering::Relaxed);
        old + amount
    }

    pub fn fetch_add(&mut self, amount: usize, ordering: Ordering) -> usize {
        self.inner.fetch_add(amount, ordering)
    }

    /// creates a reader for the value. The reader is only able to read the value, and can never mutate it
    pub fn reader(&self) -> AtomicCounterReader {
        AtomicCounterReader {
            inner: self.inner.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub struct AtomicCounterReader {
    inner: Arc<AtomicUsize>
}

impl AtomicCounterReader {
    /// Convenience method to load the value with relaxed Ordering.
    pub fn load_relaxed(&self) -> usize {
        self.load(Ordering::Relaxed)
    }

    pub fn load(&self, ordering: Ordering) -> usize {
        self.inner.load(ordering)
    }
}

