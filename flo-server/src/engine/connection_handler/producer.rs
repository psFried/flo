use std::io;
use std::error::Error;

use protocol::*;
use futures::{Future, Poll, Async};

use engine::event_stream::partition::{ProduceResponseReceiver};
use engine::ConnectionHandlerResult;
use engine::connection_handler::connection_state::ConnectionState;


#[derive(Debug)]
pub struct ProducerConnectionState {
    produce_operation: Option<(u32, ProduceResponseReceiver)>,
}


impl ProducerConnectionState {
    pub fn new() -> ProducerConnectionState {
        ProducerConnectionState {
            produce_operation: None,
        }
    }

    pub fn requires_poll_complete(&self) -> bool {
        self.produce_operation.is_some()
    }


    pub fn handle_produce(&mut self, produce: ProduceEvent, common_state: &mut ConnectionState) -> ConnectionHandlerResult {
        let op_id = produce.op_id;
        let connection_id = common_state.connection_id;

        let receiver = {
            let partition = common_state.event_stream.get_partition(produce.partition).unwrap();
            partition.produce(connection_id, op_id, vec![produce]).map_err(|err| {
                format!("Failed to send operation: {:?}", err.0)
            })?
        };

        self.produce_operation = Some((op_id, receiver));

        Ok(())
    }


    pub fn poll_produce_complete(&mut self, common_state: &mut ConnectionState) -> Poll<(), io::Error> {
        let response = match self.produce_operation {
            Some((op_id, ref mut pending)) => {
                let result = try_ready!(pending.poll().map_err(|recv_err| {
                    error!("Failed to poll produce operation for client: op_id: {}: {:?}", op_id, recv_err);
                    io::Error::new(io::ErrorKind::Other, "failed to poll produce operation")
                }));

                match result {
                    Ok(id) => {
                        ProtocolMessage::AckEvent(EventAck{
                            op_id: op_id,
                            event_id: id,
                        })
                    }
                    Err(io_err) => {
                        ProtocolMessage::Error(ErrorMessage {
                            op_id: op_id,
                            kind: ErrorKind::StorageEngineError,
                            description: format!("Persistence Error: {}", io_err.description()),
                        })
                    }
                }
            },
            None => return Ok(Async::Ready(()))
        };

        self.produce_operation = None;

        common_state.send_to_client(response).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        Ok(Async::Ready(()))
    }
}
