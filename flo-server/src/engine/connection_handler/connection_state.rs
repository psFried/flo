
use tokio_core::reactor::Handle;

use protocol::*;

use engine::{ConnectionId, ClientSender, EngineRef, SendProtocolMessage};
use engine::event_stream::EventStreamRef;

use super::ConnectionHandlerResult;

const DEFAULT_CONSUME_BATCH_SIZE: u32 = 10_000;

#[derive(Debug)]
pub struct ConnectionState {
    pub client_name: Option<String>,
    pub connection_id: ConnectionId,
    pub client_sender: ClientSender,
    pub engine: EngineRef,
    pub event_stream: EventStreamRef,
    pub reactor: Handle,
    pub consume_batch_size: u32,
}


impl ConnectionState {
    pub fn new(connection_id: ConnectionId, client_sender: ClientSender, engine: EngineRef, reactor: Handle) -> ConnectionState {
        let event_stream = engine.get_default_stream();
        debug!("Starting connection_id: {} with event_stream: {}", connection_id, event_stream.name());

        ConnectionState {
            client_name: None,
            connection_id,
            client_sender,
            engine,
            reactor,
            event_stream,
            consume_batch_size: DEFAULT_CONSUME_BATCH_SIZE,
        }
    }

    pub fn handle_announce_message(&mut self, announce: ClientAnnounce) -> ConnectionHandlerResult {
        let ClientAnnounce {op_id, client_name, consume_batch_size, ..} = announce;
        // todo: return error if client name is already set or if protocol version != 1
        self.client_name = Some(client_name);

        if let Some(batch_size) = consume_batch_size {
            debug!("Using consume batch size of {} for connection_id: {}", batch_size, self.connection_id);
            self.consume_batch_size = batch_size;
        }
        self.send_stream_status(op_id)
    }

    pub fn send_stream_status(&mut self, op_id: u32) -> ConnectionHandlerResult {
        let status = create_stream_status(op_id, &self.event_stream);
        self.send_to_client(ProtocolMessage::StreamStatus(status)).map_err(|err| {
            format!("Error sending message to client: {:?}", err)
        })
    }

    pub fn set_event_stream(&mut self, op_id: u32, name: String) -> ConnectionHandlerResult {
        use engine::ConnectError;
        trace!("attempting to set event stream for {:?} to '{}'", self, name);
        match self.engine.get_stream(&name) {
            Ok(new_stream) => {
                debug!("Setting event stream to '{}' for {:?}", new_stream.name(), self);
                let stream_status = create_stream_status(op_id, &new_stream);
                self.event_stream = new_stream;
                self.send_to_client(ProtocolMessage::StreamStatus(stream_status))
            }
            Err(ConnectError::NoStream) => {
                let err_message = ErrorMessage {
                    op_id: op_id,
                    kind: ErrorKind::NoSuchStream,
                    description: format!("Event stream: '{}' does not exist", name),
                };
                self.send_to_client(ProtocolMessage::Error(err_message))
            }
            Err(ConnectError::InitFailed(io_err)) => {
                let err_message = ErrorMessage {
                    op_id: op_id,
                    kind: ErrorKind::StorageEngineError,
                    description: format!("Failed to create stream: '{}': {:?}", name, io_err)
                };
                self.send_to_client(ProtocolMessage::Error(err_message))
            }
        }
    }

    pub fn send_to_client(&self, message: SendProtocolMessage) -> ConnectionHandlerResult {
        self.client_sender.unbounded_send(message).map_err(|e| {
            format!("Error sending outgoing message for connection_id: {}, message: {:?}", self.connection_id, e.into_inner())
        })
    }
}

fn create_stream_status(op_id: u32, stream_ref: &EventStreamRef) -> EventStreamStatus {
    let mut partition_statuses = Vec::with_capacity(stream_ref.get_partition_count() as usize);

    for partition in stream_ref.partitions() {
        let num = partition.partition_num();
        let head = partition.get_highest_event_counter();
        let primary = partition.is_primary();
        let part_status = PartitionStatus {
            partition_num: num,
            head: head,
            primary: primary,
        };
        partition_statuses.push(part_status);
    }

    EventStreamStatus {
        op_id: op_id,
        name: stream_ref.name().to_owned(),
        partitions: partition_statuses,
    }
}

