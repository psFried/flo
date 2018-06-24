use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InvalidCrcError{
    pub computed: u32,
    pub actual: u32
}

impl Display for InvalidCrcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Invalid CRC32C Checksum, expected: {}, actual: {}", self.computed, self.actual)
    }
}

impl Error for InvalidCrcError {
    fn description(&self) -> &str {
        "Invalid CRC32C Checksum"
    }
}
