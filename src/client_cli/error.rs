use std::fmt::Display;
use std::io::{self, Write};

pub trait Critical<T> {
    fn or_abort_process(self) -> T;
    fn or_abort_with_message<M: Display>(self, message: M) -> T;
}

/// Aborts the entire process immediately. This method will never return.
pub fn abort_due_to_fatal_error<E: Display>(error_message: E) {
    let mut stderr = io::stderr();
    let _ = write!(stderr, "Fatal Error: {}", error_message);

    ::std::process::exit(1);
}

impl <T, E> Critical<T> for Result<T, E> where E: ?Display {
    fn or_abort_process(self) -> T {
        match self {
            Ok(value) => value,
            Err(_ignore) => {
                abort_due_to_fatal_error("Error");
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
    fn or_abort_with_message<M: Display>(self, message: M) -> T {
        match self {
            Ok(value) => value,
            Err(_ignore) => {
                abort_due_to_fatal_error(message);
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
}

impl <T, E> Critical<T> for Result<T, E> where E: Display {
    fn or_abort_process(self) -> T {
        match self {
            Ok(value) => value,
            Err(err_message) => {
                abort_due_to_fatal_error(err_message);
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
    fn or_abort_with_message<M: Display>(self, message: M) -> T {
        match self {
            Ok(value) => value,
            Err(_ignore) => {
                abort_due_to_fatal_error(message);
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
}

impl <T> Critical<T> for Option<T> {
    fn or_abort_process(self) -> T {
        match self {
            Some(value) => value,
            None => {
                abort_due_to_fatal_error("Error");
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
    fn or_abort_with_message<M: Display>(self, message: M) -> T {
        match self {
            Some(value) => value,
            None => {
                abort_due_to_fatal_error(message);
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
}
