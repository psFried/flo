
use event::{EventCounter, ActorId};
use protocol::FloInstanceId;
use atomics::{AtomicCounterReader, AtomicCounterWriter};



pub struct CommitManager {
    /// The current commit index. Cannot ever go backwards
    commit_index: AtomicCounterWriter,
    /// The number of acknowledgements that must be received from peers in order to get a majority.
    /// Note that this number does _not_ include the implicit acknowledgement from this instance.
    /// Once this number of acks has been received from peers, the event will be considered committed
    min_required_for_commit: ActorId,
    /// The _other_ members of this cluster. Does not include this instance
    peers: Vec<(FloInstanceId, EventCounter)>,
}


impl CommitManager {

    pub fn new(commit_index: AtomicCounterWriter) -> CommitManager {
        CommitManager {
            commit_index,
            min_required_for_commit: 0,
            peers: Vec::new(),
        }
    }

    pub fn acknowledgement_received(&mut self, from: FloInstanceId, acknowledged: EventCounter) -> Option<EventCounter> {
        let peer_count = self.peers.len();

        for i in 0..peer_count {
            let &mut (ref id, ref mut counter) = &mut self.peers[i];
            if *id == from {
                *counter = acknowledged;

            }
        }

        self.peers.sort_by_key(|elem| elem.1);

        let idx = self.min_required_for_commit;
        let ack_counter = self.peers[idx as usize].1;

        if self.commit_index.set_if_greater(ack_counter as usize) {
            Some(ack_counter)
        } else {
            None
        }
    }

    pub fn update_commit_index(&mut self, new_index: EventCounter) {
        self.commit_index.set_if_greater(new_index as usize);
    }

    pub fn add_member(&mut self, peer_id: FloInstanceId) {
        self.peers.push((peer_id, 0));
        let new_ack_requirement = self.compute_min_required();
        self.min_required_for_commit = new_ack_requirement;
    }

    pub fn get_commit_index_reader(&self) -> AtomicCounterReader {
        self.commit_index.reader()
    }

    fn compute_min_required(&self) -> ActorId {
        let peer_count = self.peers.len() as ActorId;

        ::engine::minimum_required_votes_for_majority(peer_count)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn subject_with_peers(peers: &[FloInstanceId]) -> CommitManager {
        let mut subject = CommitManager::new(AtomicCounterWriter::with_value(0));
        for id in peers {
            subject.add_member(*id);
        }
        subject
    }

    #[test]
    fn commit_index_is_incremented_when_a_majority_of_all_members_have_acknowledged_an_event_greater_than_the_one_acknowledged() {
        let peers = [
            FloInstanceId::generate_new(),
            FloInstanceId::generate_new(),
            FloInstanceId::generate_new(),
            FloInstanceId::generate_new()
        ];
        let mut subject = subject_with_peers(&peers[..]);
        let commit_reader = subject.get_commit_index_reader();

        assert!(subject.acknowledgement_received(peers[0], 25).is_none());
        let result = subject.acknowledgement_received(peers[1], 17);
        assert_eq!(Some(17), result);
        assert_eq!(17, commit_reader.load_relaxed() as u64);

        let result = subject.acknowledgement_received(peers[2], 19);
        assert_eq!(Some(19), result);
        assert_eq!(19, commit_reader.load_relaxed() as u64);

        // here's the tricky part. When peer[3] acknowledges event 30, the commit index should only jump to 25
        let result = subject.acknowledgement_received(peers[3], 30);
        assert_eq!(Some(25), result);
        assert_eq!(25, commit_reader.load_relaxed() as u64);
    }

    #[test]
    fn commit_index_is_incremented_when_a_majority_of_all_members_have_acknowledged_a_specific_event() {
        let peers = [
            FloInstanceId::generate_new(),
            FloInstanceId::generate_new(),
            FloInstanceId::generate_new(),
            FloInstanceId::generate_new()
        ];
        let mut subject = subject_with_peers(&peers[..]);

        let event = 5;
        assert!(subject.acknowledgement_received(peers[0], event).is_none());
        let result = subject.acknowledgement_received(peers[1], event);
        assert_eq!(Some(event), result);
        assert_eq!(event, subject.get_commit_index_reader().load_relaxed() as u64);
    }

    #[test]
    fn adding_members_sets_then_minimum_required_for_commit() {
        let mut subject = CommitManager::new(AtomicCounterWriter::with_value(0));

        assert_eq!(0, subject.min_required_for_commit);

        subject.add_member(FloInstanceId::generate_new());
        assert_eq!(1, subject.peers.len());
        assert_eq!(1, subject.min_required_for_commit); // 2 of 2

        subject.add_member(FloInstanceId::generate_new());
        assert_eq!(2, subject.peers.len());
        assert_eq!(1, subject.min_required_for_commit); // 2 of 3

        subject.add_member(FloInstanceId::generate_new());
        assert_eq!(3, subject.peers.len());
        assert_eq!(2, subject.min_required_for_commit); // 3 of 4

        subject.add_member(FloInstanceId::generate_new());
        assert_eq!(4, subject.peers.len());
        assert_eq!(2, subject.min_required_for_commit); // 3 of 5

        subject.add_member(FloInstanceId::generate_new());
        subject.add_member(FloInstanceId::generate_new());
        subject.add_member(FloInstanceId::generate_new());

        assert_eq!(4, subject.min_required_for_commit); // 5 of 8
    }
}


