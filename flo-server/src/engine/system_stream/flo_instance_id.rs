
use std::fmt::{self, Display};


/// An opaque identifier used to uniquely identify flo instances.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct FloInstanceId(u64);

impl FloInstanceId {

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn from_u64(int_value: u64) -> FloInstanceId {
        FloInstanceId(int_value)
    }

    pub fn generate() -> FloInstanceId {
        let num = {
            let time = ::event::time::now();
            let low = time.timestamp_subsec_nanos();
            let high = time.num_seconds_from_unix_epoch();
            (high as u64) << 32 & low as u64
        };
        FloInstanceId(num)
    }
}

impl Display for FloInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Display will just look the same as Debug for now
        write!(f, "{:?}", self)
    }
}
