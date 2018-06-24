mod file;

use std::net::SocketAddr;
use std::collections::HashMap;

use event::{ActorId, EventCounter};
use protocol::Term;
use protocol::flo_instance_id::{self, FloInstanceId};
use engine::controller::controller_messages::Peer;
use engine::controller::system_event::*;
use super::SharedClusterState;

pub use self::file::FilePersistedState;
use engine::controller::controller_state::SystemEventRef;

/// Holds all the cluster state that we want to survive a reboot.
/// We always persist the `FloInstanceId` because we prefer that to be stable across reboots. We do _not_ want to persist
/// the `SocketAddr` for the server, though, since that may well change after a restart, depending on environment.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PersistentClusterState {
    pub current_term: Term,
    pub voted_for: Option<FloInstanceId>,
    pub this_instance_id: FloInstanceId,
    pub this_partition_num: Option<ActorId>,
    last_applied: EventCounter,
    last_applied_term: Term,
    cluster_members: HashMap<FloInstanceId, SocketAddr>,
    assigned_partitions: HashMap<ActorId, FloInstanceId>,
}

impl PersistentClusterState {
    /// called during system startup to initialize the shared cluster state that will be available to all the connection handlers
    pub fn initialize_shared_state(&self, this_address: Option<SocketAddr>) -> SharedClusterState {

        SharedClusterState {
            this_partition_num: self.this_partition_num,
            this_instance_id: self.this_instance_id,
            this_address,
            system_primary: None, // we are still starting up, so we have no idea who's primary
            peers: self.cluster_members.iter().map(|(id, address)| Peer{id: *id, address: *address}).collect()
        }
    }

    pub fn generate_new() -> PersistentClusterState {
        PersistentClusterState {
            last_applied: 0,
            last_applied_term: 0,
            current_term: 0,
            voted_for: None,
            this_instance_id: flo_instance_id::generate_new(),
            this_partition_num: None,
            cluster_members: HashMap::new(),
            assigned_partitions: HashMap::new(),
        }
    }

    pub fn apply_system_event(&mut self, event: SystemEventRef) {
        let counter = event.counter;
        let term = event.term();
        self.current_term = self.current_term.max(term);

        if counter > self.last_applied {
            match event.data.kind {
                SystemEventKind::ClusterInitialized(ref initial_membership) => {
                    for peer in initial_membership.peers.iter() {
                        self.add_peer(peer);
                    }
                }
                SystemEventKind::NewClusterMemberJoining(ref new_peer) => {
                    self.add_peer(new_peer);
                }
                SystemEventKind::AssignPartition(ref assigned) => {
                    let peer_id = assigned.peer_id;
                    if !self.cluster_members.contains_key(&peer_id) {
                        error!("Fatal error: AssignPartition event refers to an unknown peer_id, which indicates \
                            that the system log is inconsistent! Offending event counter: {} term: {}, kind: \
                            AssignPartition({:?}), current state: {:?}", counter, term, assigned, self);
                        panic!("System controller encountered a fatal error");
                    }
                    self.assigned_partitions.insert(assigned.partition_num, peer_id);
                }
            }
            self.last_applied = counter;
        }

    }

    pub fn get_last_applied(&self) -> (EventCounter, Term) {
        (self.last_applied, self.last_applied_term)
    }

    pub fn add_peer(&mut self, peer: &Peer) {
        self.cluster_members.insert(peer.id, peer.address);
    }

    pub fn contains_peer(&self, peer_id: FloInstanceId) -> bool {
        self.cluster_members.contains_key(&peer_id)
    }

    pub fn get_all_peer_ids(&self) -> impl Iterator<Item=&FloInstanceId> {
        self.cluster_members.keys()
    }

    pub fn get_all_peer_addresses(&self) -> impl Iterator<Item=&SocketAddr> {
        self.cluster_members.values()
    }

    pub fn get_all_peers(&self) -> Vec<Peer> {
        self.cluster_members.iter().map(|(id, address)| {
            Peer {id: *id, address: *address}
        }).collect()
    }

    pub fn get_voting_peer_count(&self) -> ActorId {
        self.cluster_members.len() as ActorId
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use test_utils::addr;
    use std::collections::HashSet;

    #[test]
    #[should_panic]
    fn applying_partition_assigned_panics_when_peer_is_not_a_cluster_member() {
        let mut subject = subject_with_initial_peers(initial_peers());
        let event = sys_event(2, 7, SystemEventKind::AssignPartition(PartitionAssigned {
            peer_id: flo_instance_id::generate_new(),
            partition_num: 1,
        }));
        subject.apply_system_event(event);
    }

    #[test]
    fn applying_partition_assigned_adds_member_to_the_assigned_members_map() {
        let initial_peers = initial_peers();
        let mut subject = subject_with_initial_peers(initial_peers.clone());
        assert!(subject.assigned_partitions.is_empty());

        let event = sys_event(2, 7, SystemEventKind::AssignPartition(PartitionAssigned {
            peer_id: initial_peers[0].id,
            partition_num: 3,
        }));
        subject.apply_system_event(event);

        assert_eq!(1, subject.assigned_partitions.len());
        assert_eq!(Some(initial_peers[0].id), subject.assigned_partitions.get(&3).cloned());
        assert_eq!(2, subject.last_applied);
        assert_eq!(7, subject.current_term);
    }

    #[test]
    fn applying_new_member_joining_adds_a_cluster_member() {
        let initial_peers = initial_peers();
        let mut subject = subject_with_initial_peers(initial_peers.clone());
        let new_peer = new_peer(4);
        let event = sys_event(2, 7, SystemEventKind::NewClusterMemberJoining(new_peer.clone()));
        subject.apply_system_event(event);

        assert_eq!(2, subject.last_applied);
        assert_eq!(7, subject.current_term);

        let mut expected_peers = initial_peers;
        expected_peers.push(new_peer);
        assert_peer_groups_equal(expected_peers, subject.get_all_peers());
        assert!(subject.assigned_partitions.is_empty());
    }

    #[test]
    fn applying_initial_membership_sets_cluster_members() {
        let peers = initial_peers();
        let subject = subject_with_initial_peers(peers.clone());

        assert_eq!(1, subject.last_applied);
        assert_eq!(1, subject.current_term);
        assert_peer_groups_equal(peers, subject.get_all_peers());
        assert!(subject.assigned_partitions.is_empty());
    }

    fn assert_peer_groups_equal<E: IntoIterator<Item=Peer>, A: IntoIterator<Item=Peer>>(e: E, a: A) {
        let actual = a.into_iter().collect::<HashSet<Peer>>();
        let expected = e.into_iter().collect::<HashSet<Peer>>();
        assert_eq!(expected, actual);
    }

    fn subject_with_initial_peers(peers: Vec<Peer>) -> PersistentClusterState {
        let mut subject = PersistentClusterState::generate_new();
        let event = sys_event(1, 1, SystemEventKind::ClusterInitialized(InitialClusterMembership {
            peers
        }));
        subject.apply_system_event(event);
        subject
    }

    fn initial_peers() -> Vec<Peer> {
        vec![
            new_peer(3000),
            new_peer(3001),
            new_peer(3002),
        ]
    }

    fn new_peer(port: u16) -> Peer {
        Peer { id: flo_instance_id::generate_new(), address: addr(&format!("127.0.0.1:{}", port)) }
    }

    fn sys_event(counter: EventCounter, term: Term, kind: SystemEventKind) -> SystemEventRef {
        let data = SystemEventData { term, kind };
        SystemEventRef { counter, data }
    }
}
