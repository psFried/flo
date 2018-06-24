use std::io;
use std::collections::VecDeque;

use futures::sync::oneshot::Sender;

use event::{FloEventId, ActorId, EventCounter};

struct Pending {
    sender: Sender<io::Result<FloEventId>>,
    op_id: u32,
    event: EventCounter,
}

pub struct PendingProduceOperations {
    partition: ActorId,
    pending: VecDeque<Pending>,
}


impl PendingProduceOperations {

    pub fn new(partition: ActorId) -> PendingProduceOperations {
        PendingProduceOperations {
            partition,
            pending: VecDeque::new(), // Don't allocate any space, since we may just be in standalone mode
        }
    }

    pub fn add(&mut self, op_id: u32, event: EventCounter, sender: Sender<io::Result<FloEventId>>) {
        self.pending.push_back(Pending {
            sender, op_id, event
        });
    }

    pub fn commit_success(&mut self, commit_index: EventCounter) {
        let count = self.pending.iter().take_while(|op| {
            op.event <= commit_index
        }).count();
        debug!("New commit index of {} will complete {} pending operations", commit_index, count);

        let partition = self.partition;
        for _ in 0..count {
            let op = self.pending.pop_front().unwrap();
            debug!("Notifying producer of committed op_id: {}, event: {}", op.op_id, op.event);
            let _ = op.sender.send(Ok(FloEventId::new(partition, op.event)));
        }
    }

    // just a guess at the api we'll probably want here
//    pub fn commit_fail(&mut self, fail_through: EventCounter) {
//
//    }
}
