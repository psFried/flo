use super::{FloCliCommand, Context as CliContext};
use flo_client_lib::codec::LossyStringCodec;
use flo_client_lib::sync::{SyncConnection, HandshakeError, ErrorType};
use flo_client_lib::{Event, FloEventId, VersionVector};

use std::fmt::{self, Display};

pub struct CliConsumerOptions {
    pub event_stream: Option<String>,
    pub host: String,
    pub port: u16,
    pub namespace: String,
    //TODO: allow passing multiple start position arguments so we can properly use a VersionVector
    pub start_position: Option<FloEventId>,
    pub limit: Option<u64>,
    pub await: bool,
    pub batch_size: Option<u32>,
}

pub struct CliConsumer;

impl FloCliCommand for CliConsumer {
    type Input = CliConsumerOptions;
    type Error = ConsumerError;

    fn run(input: Self::Input, output: &CliContext) -> Result<(), Self::Error> {
        let CliConsumerOptions { host, port, event_stream, namespace, limit, await, start_position, batch_size} = input;

        let address = format!("{}:{}", host, port);

        output.verbose(format!("Connecting to: {}", &address));
        let mut connection = SyncConnection::connect_from_str(&address, "flo-client-cli", LossyStringCodec, batch_size)?;

        if let Some(stream) = event_stream {
            connection.set_event_stream(stream)?;
        }
        {
            let status = connection.current_stream().unwrap();
            output.debug(format!("stream status: {:?}", status));
            output.normal(format!("Using event stream: '{}'", &status.name));
        }

        let mut version_vector = VersionVector::new();
        if let Some(id) = start_position {
            version_vector.set(id);
        } else {
            // safe unwrap since connect succeeded
            let current_stream = connection.current_stream().unwrap();
            // set the version vector to just start at the beginning for all partitions
            for partition in current_stream.partitions.iter() {
                version_vector.set(FloEventId::new(partition.partition_num, 0));
            }
        }


        let event_iter = connection.into_consumer(namespace, &version_vector, limit, await);
        for result in event_iter {
            let event = result?;
            print_event(output, event);
        }
        Ok(())
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

pub struct ConsumerError(ErrorType);


impl From<HandshakeError> for ConsumerError {
    fn from(HandshakeError{error_type, ..}: HandshakeError) -> Self {
        ConsumerError(error_type)
    }
}

impl From<ErrorType> for ConsumerError {
    fn from(err_type: ErrorType) -> Self {
        ConsumerError(err_type)
    }
}

impl Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            ErrorType::Io(ref io_err) => {
                write!(f, "I/O Error: {:?} - {}", io_err.kind(), io_err)
            }
            ErrorType::Server(ref err_message) => {
                write!(f, "Received Error: {:?} - {}", err_message.kind, err_message.description)
            }
            ErrorType::Codec(_) => {
                // this is not reachable since we are using the LossyStringCodec, which cannot return an error
                unreachable!()
            }
        }
    }
}
