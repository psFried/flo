mod producer;
mod consumer;

pub use self::producer::{Producer, ProduceOptions};
pub use self::consumer::{CliConsumerOptions, CliConsumer};

use std::io::Write;
use std::fmt::Display;

pub trait FloCliCommand {
    type Error: Display;
    type Input;

    fn run(input: Self::Input, output: &Context) -> Result<(), Self::Error>;
}



pub fn run<T: FloCliCommand>(input: T::Input, context: Context) {
    match T::run(input, &context) {
        Ok(()) => {
            context.exit_gracefully();
        },
        Err(err_message) => {
            context.abort_process(err_message);
        }
    }

}



#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Verbosity {
    Debug,
    Verbose,
    Normal,
    Error,
}

impl Verbosity {
    fn ordinal(&self) -> u8 {
        use self::Verbosity::*;
        match *self {
            Debug => 4,
            Verbose => 3,
            Normal => 2,
            Error => 1,
        }
    }

    fn is_greater_or_equal(&self, other: Verbosity) -> bool {
        self.ordinal() >= other.ordinal()
    }
}

pub struct Context {
    verbosity: Verbosity,
}

impl Context {
    pub fn new(verbosity: Verbosity) -> Context {
        Context {
            verbosity: verbosity
        }
    }

    pub fn verbose<M: Display>(&self, message: M) {
        self.println(message, Verbosity::Verbose);
    }

    pub fn normal<M: Display>(&self, message: M) {
        self.println(message, Verbosity::Normal);
    }

    pub fn error<M: Display>(&self, message: M) {
        self.write_stderr(message, Verbosity::Error);
    }

    pub fn write_stdout<M: Display>(&self, message: M, verbosity: Verbosity) {
        if self.verbosity.is_greater_or_equal(verbosity) {
            let mut stdout = ::std::io::stdout();
            write!(stdout, "{}", message).unwrap();
            stdout.flush().unwrap();
        }
    }

    pub fn println<M: Display>(&self, message: M, verbosity: Verbosity) {
        self.write_stdout(message, verbosity);
        self.write_stdout('\n', verbosity);
    }

    pub fn write_stderr<M: Display>(&self, message: M, verbosity: Verbosity) {
        if self.verbosity.is_greater_or_equal(verbosity) {
            let mut stderr = ::std::io::stderr();
            write!(stderr, "{}", message).unwrap();
            stderr.flush().unwrap();
        }
    }

    pub fn exit_gracefully(&self) {
        ::std::process::exit(0);
    }

    pub fn abort_process<M: Display>(&self, message: M) {
        self.error(message);
        ::std::process::exit(1);
    }
}

pub trait Critical<T> {
    fn or_abort_process(self, context: &Context) -> T;
    fn or_abort_with_message<M: Display>(self, message: M, context: &Context) -> T;
}

impl <T, E> Critical<T> for Result<T, E> where E: Display {
    fn or_abort_process(self, context: &Context) -> T {
        match self {
            Ok(value) => value,
            Err(err_message) => {
                context.abort_process(err_message);
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
    fn or_abort_with_message<M: Display>(self, message: M, context: &Context) -> T {
        match self {
            Ok(value) => value,
            Err(_ignore) => {
                context.abort_process(message);
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
}

impl <T> Critical<T> for Option<T> {
    fn or_abort_process(self, context: &Context) -> T {
        match self {
            Some(value) => value,
            None => {
                context.abort_process("Error");
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
    fn or_abort_with_message<M: Display>(self, message: M, context: &Context) -> T {
        match self {
            Some(value) => value,
            None => {
                context.abort_process(message);
                panic!("this statement is unreachable but required to appease the compiler");
            }
        }
    }
}


