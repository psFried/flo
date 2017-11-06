pub mod consumer_stream;
pub mod pending_consume;

use std::io;

use futures::{Stream, Future, Async, Poll};

use event::ActorId;
use protocol::*;
use new_engine::connection_handler::ConnectionHandlerResult;
use new_engine::connection_handler::connection_state::ConnectionState;
use new_engine::event_stream::partition::{PartitionReader, EventFilter};

use self::consumer_stream::{Consumer,
                            ConsumerStatus,
                            ConsumerStatusSetter,
                            create_status_channel};

use self::pending_consume::PendingConsumeOperation;


#[derive(Debug)]
struct ActiveConsumer {
    status_setter: ConsumerStatusSetter,
    partitions: Vec<ActorId>,
}

#[derive(Debug)]
pub struct ConsumerConnectionState {
    pending_consume_operation: Option<PendingConsumeOperation>,
    consumer_ref: Option<ActiveConsumer>,
}


impl ConsumerConnectionState {
    pub fn new() -> ConsumerConnectionState {
        ConsumerConnectionState {
            pending_consume_operation: None,
            consumer_ref: None,
        }
    }

    pub fn shutdown(&mut self, connection: &mut ConnectionState) {
        if let Some(ref mut consumer) = self.consumer_ref {
            // tell the active consumer to stop sending events
            consumer.status_setter.set(ConsumerStatus::Stop);

            // tell the partitions to remove their consumer notifiers
            let connection_id = connection.connection_id;
            for partition_num in consumer.partitions.iter() {
                if let Some(partition_ref) = connection.event_stream.get_partition(*partition_num) {
                    debug!("Sending consumer stop to partition: {} for connection_id: {}", partition_num, connection_id);
                    partition_ref.stop_consuming(connection_id)
                }
            }
        }
    }

    pub fn requires_poll_complete(&self) -> bool {
        self.pending_consume_operation.is_some()
    }

    pub fn handle_next_batch(&mut self, connection: &mut ConnectionState) -> ConnectionHandlerResult {
        if let Some(ref mut active_consumer) = self.consumer_ref {
            debug!("Setting NextBatch status for consumer for connection_id: {}", connection.connection_id);
            active_consumer.status_setter.set(ConsumerStatus::NextBatch);
        } else {
            warn!("Ignoring NextBatch message for connection_id: {} since no active consumer is in progress", connection.connection_id);
        }
        Ok(())
    }

    pub fn handle_start_consuming(&mut self, start: NewConsumerStart, connection: &mut ConnectionState) -> ConnectionHandlerResult {
        let NewConsumerStart {op_id, version_vector, namespace, max_events} = start;
        let event_limit = if max_events == u64::max_value() {
            None
        } else {
            Some(max_events)
        };

        match EventFilter::parse(&namespace) {
            Ok(filter) => {
                let connection_id = connection.connection_id;
                let mut pending_consume = PendingConsumeOperation::new(op_id, event_limit);

                for id in version_vector {
                    let start = id.event_counter;
                    let partition = id.actor;
                    let notifier = pending_consume.create_notifier(connection_id);

                    let send_result = connection.event_stream.get_partition(partition).unwrap().consume(connection_id,
                                                                                                     op_id,
                                                                                                     notifier,
                                                                                                     filter.clone(),
                                                                                                     start);

                    let receiver = send_result.map_err(|err| {
                        format!("Failed to send consume operation to partition: {} : {:?}", partition, err)
                    })?;

                    pending_consume.add_partition(partition, receiver);
                }
                self.pending_consume_operation = Some(pending_consume);
                self.poll_pending_consume(connection)
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

    pub fn poll_pending_consume(&mut self, connection: &mut ConnectionState) -> ConnectionHandlerResult {
        self.poll_consume_complete(connection).map_err(|io_err| {
            format!("Error polling pending consume: {:?}", io_err)
        }).map(|_| {
            ()
        })
    }

    pub fn poll_consume_complete(&mut self, connection: &mut ConnectionState) -> Poll<(), io::Error> {
        let readers = if let Some(ref mut pending) = self.pending_consume_operation {
            try_ready!(pending.poll_ready())
        } else {
            return Ok(Async::Ready(()));
        };

        self.spawn_consumer(readers, connection)
    }


    fn spawn_consumer(&mut self, readers: Vec<PartitionReader>, connection: &mut ConnectionState) -> Poll<(), io::Error> {
        let pending = self.pending_consume_operation.take().unwrap();
        let partition_numbers = pending.get_partition_numbers();
        let PendingConsumeOperation {op_id, task_setter, max_events, ..} = pending;

        let batch_size = connection.consume_batch_size;
        let send_result = connection.send_to_client(ProtocolMessage::CursorCreated(CursorInfo {
            op_id: op_id,
            batch_size: batch_size,
        }));

        if let Err(desc) = send_result {
            return Err(io::Error::new(io::ErrorKind::Other, desc));
        }

        let (status_setter, status_checker) = create_status_channel();

        let connection_id = connection.connection_id;
        let consumer = Consumer::new(connection_id, batch_size, status_checker, task_setter, readers, op_id, max_events);
        let future = consumer.forward(connection.client_sender.clone()).map_err(move |err| {
            error!("Consumer failed for connection_id: {}, op_id: {}, err: {:?}", connection_id, op_id, err);
            ()
        }).map(move |_| {
            debug!("Successfully finished consumer for connection_id: {}, op_id: {}", connection_id, op_id);
            ()
        });

        let active_consumer = ActiveConsumer {
            status_setter: status_setter,
            partitions: partition_numbers,
        };
        self.consumer_ref = Some(active_consumer);
        connection.reactor.spawn(future);

        Ok(Async::Ready(()))
    }
}

