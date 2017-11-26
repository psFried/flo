
use std::net::SocketAddr;
use std::fmt::{self, Display};


/// An opaque identifier used to uniquely identify flo instances.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct FloInstanceId(SocketAddr);

impl FloInstanceId {

    pub fn new(addr: SocketAddr) -> Self {
        FloInstanceId(addr)
    }
}

impl Display for FloInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Display will just look the same as Debug for now
        write!(f, "{}", self.0)
    }
}
