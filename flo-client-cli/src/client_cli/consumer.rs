use super::{FloCliCommand, Context as CliContext, Verbosity};
use flo_client_lib::sync::{
    Consumer,
    Context,
    ConsumerAction,
    ClientError,
};
use flo_client_lib::codec::LossyStringCodec;
use flo_client_lib::sync::connection::{SyncConnection, ConsumerOptions};
use flo_client_lib::{Event, FloEventId, VersionVector};

use std::fmt::{self, Display};
use std::io;

pub struct CliConsumerOptions {
    pub host: String,
    pub port: u16,
    pub namespace: String,
    //TODO: allow passing multiple start position arguments so we can properly use a VersionVector
    pub start_position: Option<FloEventId>,
    pub limit: Option<u64>,
    pub await: bool,
}

pub struct CliConsumer;

impl FloCliCommand for CliConsumer {
    type Input = CliConsumerOptions;
    type Error = ConsumerError;

    fn run(input: Self::Input, output: &CliContext) -> Result<(), Self::Error> {
        let CliConsumerOptions { host, port, namespace, limit, await, start_position} = input;

        let mut version_vector = VersionVector::new();
        if let Some(id) = start_position {
            version_vector.set(id);
        }

        let consumer_opts = ConsumerOptions::new(namespace, version_vector, limit.unwrap_or(::std::u64::MAX), await);

        let address = format!("{}:{}", host, port);

        output.verbose(format!("Connecting to: {}", &address));
        SyncConnection::connect(&address, LossyStringCodec).map_err(|io_err| io_err.into())
                .and_then(|mut connection| {
                    let mut consumer = PrintingConsumer{
                        context: &output,
                        await: await,
                    };
                    let result = connection.run_consumer(consumer_opts, &mut consumer);
                    match result {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            Err(error.into())
                        }
                    }
                })
    }
}

struct PrintingConsumer<'a>{
    context: &'a CliContext,
    await: bool,
}

impl <'a> Consumer<String> for PrintingConsumer<'a> {
    fn name(&self) -> &str {
        "FloCliConsumer"
    }

    fn on_event<C: Context<String>>(&mut self, event: Event<String>, _context: &mut C) -> ConsumerAction {
        print_event(&self.context, event);
        ConsumerAction::Continue
    }

    fn on_error(&mut self, error: &ClientError) -> ConsumerAction {
        if self.await && (error.is_timeout()) {
            self.context.write_stdout('.', Verbosity::Verbose);
            ConsumerAction::Continue
        } else if error.is_timeout() {
            ConsumerAction::Stop
        } else {
            self.context.debug(format!("Got error: {:?}", error));
            ConsumerAction::Stop
        }
    }
}

//TODO: come up with better ways to format the output. Maybe have a few different output options
fn print_event(output: &CliContext, event: Event<String>) {
    output.normal(""); //put a newline before the event to separate them
    let parent = if let Some(id) = event.parent_id {
        format!(", Parent: {}", id)
    } else {
        String::new()
    };
    output.normal(format!("EventId: {}{}\nNamespace: {}\nTimestamp: {}\nBody: {}",
                          event.id,
                          parent,
                          event.namespace,
                          event.timestamp,
                          event.data));
}

pub struct ConsumerError(ClientError);

impl From<ClientError> for ConsumerError {
    fn from(err: ClientError) -> Self {
        ConsumerError(err)
    }
}

impl From<io::Error> for ConsumerError {
    fn from(io_err: io::Error) -> Self {
        ConsumerError(ClientError::Transport(io_err))
    }
}

impl Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            ClientError::Transport(ref io_err) => {
                write!(f, "I/O Error: {:?} - {}", io_err.kind(), io_err)
            }
            ClientError::FloError(ref err_message) => {
                write!(f, "Received Error: {:?} - {}", err_message.kind, err_message.description)
            }
            ClientError::UnexpectedMessage(ref _message) => {
                write!(f, "Received Unexpected message from flo server")
            }
            ClientError::Codec(_) => {
                // this is not reachable since we are using the LossyStringCodec, which cannot return an error
                unreachable!()
            }
        }
    }
}
