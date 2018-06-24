
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct AtomicBoolWriter {
    inner: Arc<AtomicBool>
}

impl AtomicBoolWriter {
    pub fn with_value(starting: bool) -> AtomicBoolWriter {
        AtomicBoolWriter {
            inner: Arc::new(AtomicBool::new(starting))
        }
    }

    pub fn set(&mut self, value: bool) -> bool {
        self.inner.swap(value, Ordering::Relaxed)
    }

    #[allow(unused_mut)]
    pub fn get(&mut self) -> bool {
        self.inner.load(Ordering::Relaxed)
    }

    pub fn reader(&self) -> AtomicBoolReader {
        AtomicBoolReader {
            inner: self.inner.clone()
        }
    }
}


#[derive(Debug, Clone)]
pub struct AtomicBoolReader {
    inner: Arc<AtomicBool>
}

impl AtomicBoolReader {

    pub fn get_relaxed(&self) -> bool {
        self.inner.load(Ordering::Relaxed)
    }

    pub fn load(&self, ordering: Ordering) -> bool {
        self.inner.load(ordering)
    }
}
