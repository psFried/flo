use std::fmt::Display;
use std::io::{self, Write};

pub trait Critical<T> {
    fn or_abort_process(self) -> T;
}

/// Aborts the entire process immediately. This method will never return.
pub fn abort_due_to_fatal_error<E: Display>(error_message: E) {
    error!("Aborting due to fatal error: {}", error_message);

    let mut stderr = io::stderr();
    let _ = write!(stderr, "Fatal Error: {}", error_message);

    ::std::process::exit(1);
}

impl <T, E> Critical<T> for Result<T, E> where E: Display {
    fn or_abort_process(self) -> T {
        match self {
            Ok(value) => value,
            Err(err_message) => {
                abort_due_to_fatal_error(err_message);
                panic!("this statement is unreachable but required to appeas the compiler");
            }
        }
    }
}
