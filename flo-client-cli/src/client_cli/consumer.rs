use super::{FloCliCommand, Context, Verbosity};
use flo_client_lib::{SyncConnection,
    FloConsumer,
    ConsumerOptions,
    ConsumerContext,
    ConsumerAction,
    ClientError,
    FloEventId,
    OwnedFloEvent,
};

use std::fmt::{self, Display};
use std::io;

pub struct CliConsumerOptions {
    pub host: String,
    pub port: u16,
    pub namespace: String,
    pub start_position: Option<FloEventId>,
    pub limit: Option<u64>,
    pub await: bool,
}


pub struct Consumer;

impl FloCliCommand for Consumer {
    type Input = CliConsumerOptions;
    type Error = ConsumerError;

    fn run(input: Self::Input, output: &Context) -> Result<(), Self::Error> {
        let CliConsumerOptions { host, port, namespace, start_position, limit, await } = input;

        let consumer_opts = ConsumerOptions::simple(namespace, start_position, limit.unwrap_or(::std::u64::MAX));

        let address = format!("{}:{}", host, port);

        output.verbose(format!("Connecting to: {}", &address));
        SyncConnection::connect(&address).map_err(|io_err| io_err.into())
                .and_then(|mut connection| {
                    let mut consumer = PrintingConsumer{
                        context: &output,
                        await: await,
                    };
                    let result = connection.run_consumer(consumer_opts, &mut consumer);
                    match result {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            if error.is_timeout() {
                                Ok(())
                            } else {
                                Err(error.into())
                            }
                        }
                    }
                })
    }
}

struct PrintingConsumer<'a>{
    context: &'a Context,
    await: bool,
}

impl <'a> FloConsumer for PrintingConsumer<'a> {
    fn name(&self) -> &str {
        "FloCliConsumer"
    }

    fn on_event<C: ConsumerContext>(&mut self, event: Result<OwnedFloEvent, &ClientError>, _context: &mut C) -> ConsumerAction {
        match event {
            Ok(event) => {
                print_event(&self.context, event);
                ConsumerAction::Continue
            }
            Err(ref error) if (error.is_timeout() || error.is_end_of_stream()) && self.await => {
                self.context.write_stdout('.', Verbosity::Verbose);
                ConsumerAction::Continue
            }
            Err(ref error) if error.is_timeout() => {
                ConsumerAction::Stop
            }
            Err(ref error) => {
                self.context.debug(format!("Got error: {:?}", error));
                ConsumerAction::Stop
            }
        }
    }
}

//TODO: come up with better ways to format the output. Maybe have a few different output options
fn print_event(output: &Context, event: OwnedFloEvent) {
    output.normal(""); //put a newline before the event to separate them
    let parent = if let Some(id) = event.parent_id {
        format!(", Parent: {}", id)
    } else {
        String::new()
    };
    output.normal(format!("EventId: {}{}\nNamespace: {}\nTimestamp: {}", event.id, parent, event.namespace, event.timestamp));
    output.normal(String::from_utf8_lossy(&event.data));
}

pub struct ConsumerError(ClientError);

impl From<ClientError> for ConsumerError {
    fn from(err: ClientError) -> Self {
        ConsumerError(err)
    }
}

impl From<io::Error> for ConsumerError {
    fn from(io_err: io::Error) -> Self {
        ConsumerError(ClientError::from(io_err))
    }
}

impl Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            ClientError::Io(ref io_err) => {
                write!(f, "I/O Error: {:?} - {}", io_err.kind(), io_err)
            }
            ClientError::FloError(ref err_message) => {
                write!(f, "Received Error: {:?} - {}", err_message.kind, err_message.description)
            }
            ClientError::UnexpectedMessage(ref _message) => {
                write!(f, "Received Unexpected message from flo server")
            }
            ClientError::EndOfStream => {
                write!(f, "End of Stream")
            }
        }
    }
}
