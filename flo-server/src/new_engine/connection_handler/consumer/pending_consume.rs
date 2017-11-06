
use std::io;

use futures::{Future, Async, Poll};

use event::ActorId;
use new_engine::ConnectionId;
use new_engine::event_stream::partition::{ConsumeResponseReceiver, ConsumerNotifier, PartitionReader};
use new_engine::connection_handler::consumer::consumer_stream::{ConsumerTaskSetter};


#[derive(Debug)]
pub struct PendingConsumer {
    partition: ActorId,
    receiver: ConsumeResponseReceiver,
    reader: Option<PartitionReader>,
}

impl PendingConsumer {
    fn poll_ready(&mut self) -> Poll<(), io::Error> {
        if self.reader.is_some() {
            Ok(Async::Ready(()))
        } else {
            let reader = try_ready!(self.receiver.poll().map_err(|recv_err| {
                error!("Failed to poll consume operation for partition {}: {:?}", self.partition, recv_err);
                io::Error::new(io::ErrorKind::Other, "failed to poll consume operation")
            }));
            self.reader = Some(reader);
            Ok(Async::Ready(()))
        }
    }
}


#[derive(Debug)]
pub struct PendingConsumeOperation {
    pub op_id: u32,
    pub complete: bool,
    pub task_setter: ConsumerTaskSetter,
    pub max_events: Option<u64>,
    pub pending: Vec<PendingConsumer>,
}

impl PendingConsumeOperation {
    pub fn new(op_id: u32, max_events: Option<u64>) -> PendingConsumeOperation {
        PendingConsumeOperation {
            op_id,
            task_setter: ConsumerTaskSetter::create(),
            max_events,
            complete: false,
            pending: Vec::new(),
        }
    }

    pub fn create_notifier(&self, connection_id: ConnectionId) -> Box<ConsumerNotifier> {
        self.task_setter.create_notifier(connection_id)
    }

    pub fn add_partition(&mut self, partition: ActorId, receiver: ConsumeResponseReceiver) {
        self.pending.push(PendingConsumer {
            partition,
            receiver,
            reader: None
        });
    }

    pub fn poll_ready(&mut self) -> Poll<Vec<PartitionReader>, io::Error> {
        let mut all_ready = true;
        for pending in self.pending.iter_mut() {
            let result = pending.poll_ready();
            match result {
                Err(err) => {
                    self.complete = true;
                    return Err(err);
                }
                Ok(Async::NotReady) => {
                    all_ready = false;
                }
                Ok(Async::Ready(())) => { }
            }
        }

        if all_ready {
            let mut collected = Vec::with_capacity(self.pending.len());
            for done in self.pending.iter_mut() {
                collected.push(done.reader.take().unwrap());
            }
            Ok(Async::Ready(collected))
        } else {
            Ok(Async::NotReady)
        }
    }

    pub fn get_partition_numbers(&self) -> Vec<ActorId> {
        self.pending.iter().map(|p| p.partition).collect()
    }
}
