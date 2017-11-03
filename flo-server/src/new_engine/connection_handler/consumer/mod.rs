pub mod consumer_stream;

use std::io;

use futures::{Stream, Future, Async, Poll};

use protocol::*;
use new_engine::connection_handler::ConnectionHandlerResult;
use new_engine::connection_handler::connection_state::ConnectionState;
use new_engine::event_stream::partition::{PartitionReader,
                                          EventFilter,
                                          PartitionSendError,
                                          ConsumeResponseReceiver,
                                          AsyncConsumeResult,
                                          ConsumerNotifier};

use self::consumer_stream::{Consumer,
                            ConsumerTaskSetter,
                            ConsumerStatusChecker,
                            ConsumerStatusSetter,
                            PendingConsumer,
                            prepare_consumer_start,
                            create_status_channel};



const DEFAULT_CONSUME_BATCH_SIZE: u32 = 10_000;


#[derive(Debug)]
pub struct ConsumerConnectionState {
    consume_batch_size: u32,
    pending_consume_operation: Option<(PendingConsumer, ConsumeResponseReceiver)>,
    consumer_ref: Option<ConsumerStatusSetter>,
}


impl ConsumerConnectionState {
    pub fn new() -> ConsumerConnectionState {
        ConsumerConnectionState {
            consume_batch_size: DEFAULT_CONSUME_BATCH_SIZE,
            pending_consume_operation: None,
            consumer_ref: None,
        }
    }

    pub fn requires_poll_complete(&self) -> bool {
        self.pending_consume_operation.is_some()
    }

    pub fn handle_start_consuming(&mut self, start: NewConsumerStart, connection: &mut ConnectionState) -> ConnectionHandlerResult {
        let NewConsumerStart {op_id, version_vector, namespace, max_events} = start;

        match EventFilter::parse(&namespace) {
            Ok(filter) => {
                let start = version_vector.first().map(|id| id.event_counter).unwrap_or(0);
                let (pending, notifier) = prepare_consumer_start(op_id, Some(max_events));

                let result = connection.event_stream.get_partition(1).unwrap().consume(
                    connection.connection_id,
                    op_id,
                    notifier,
                    filter,
                    start);

                self.handle_consume_send_result(op_id, result, pending)
            }
            Err(description) => {
                connection.send_to_client(ProtocolMessage::Error(ErrorMessage {
                    op_id: op_id,
                    kind: ErrorKind::InvalidNamespaceGlob,
                    description: description,
                }))
            }
        }
    }

    pub fn poll_consume_complete(&mut self, connection: &mut ConnectionState) -> Poll<(), io::Error> {
        let event_reader = {
            if let Some((ref pending, ref mut recv)) = self.pending_consume_operation {
                try_ready!(recv.poll().map_err(|recv_err| {
                    error!("Failed to poll consume operation for client: op_id: {}: {:?}", pending.op_id, recv_err);
                    io::Error::new(io::ErrorKind::Other, "failed to poll consume operation")
                }))
            } else {
                unreachable!() // since we've already check to make sure consume_operation is some before calling this
            }
        };

        let (pending, _) = self.pending_consume_operation.take().unwrap();

        self.spawn_consumer(event_reader, pending, connection)
    }

    fn handle_consume_send_result(&mut self, _op_id: u32, result: AsyncConsumeResult, pending: PendingConsumer) -> ConnectionHandlerResult {
        match result {
            Ok(recv) => {
                self.pending_consume_operation = Some((pending, recv));
                Ok(())
            }
            Err(send_err) => {
                Err(format!("Failed to send operation: {:?}", send_err.0))
            }
        }
    }



    fn spawn_consumer(&mut self, reader: PartitionReader, pending: PendingConsumer, connection: &mut ConnectionState) -> Poll<(), io::Error> {
        let op_id = pending.op_id;
        let send_result = connection.send_to_client(ProtocolMessage::CursorCreated(CursorInfo {
            op_id: op_id,
            batch_size: self.consume_batch_size,
        }));

        if let Err(desc) = send_result {
            return Err(io::Error::new(io::ErrorKind::Other, desc));
        }

        let (status_setter, status_checker) = create_status_channel();

        let connection_id = connection.connection_id;
        let consumer = Consumer::new(connection_id, self.consume_batch_size, status_checker, reader, pending);
        let future = consumer.forward(connection.client_sender.clone()).map_err(move |err| {
            error!("Consumer failed for connection_id: {}, op_id: {}, err: {:?}", connection_id, op_id, err);
            ()
        }).map(move |_| {
            debug!("Successfully finished consumer for connection_id: {}, op_id: {}", connection_id, op_id);
            ()
        });

        self.consumer_ref = Some(status_setter);
        connection.reactor.spawn(future);

        Ok(Async::Ready(()))
    }
}

