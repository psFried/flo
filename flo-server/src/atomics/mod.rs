mod atomic_counter;
mod atomic_boolean;

pub use self::atomic_counter::{AtomicCounterWriter, AtomicCounterReader};
pub use self::atomic_boolean::{AtomicBoolWriter, AtomicBoolReader};


