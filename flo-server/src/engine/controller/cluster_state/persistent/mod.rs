mod file;

use std::net::SocketAddr;
use std::collections::HashMap;

use event::{ActorId, FloEvent, EventCounter};
use protocol::Term;
use protocol::flo_instance_id::{self, FloInstanceId};
use engine::controller::controller_messages::Peer;
use engine::controller::system_event::*;
use super::SharedClusterState;

pub use self::file::FilePersistedState;

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
            current_term: 0,
            voted_for: None,
            this_instance_id: flo_instance_id::generate_new(),
            this_partition_num: None,
            cluster_members: HashMap::new(),
            assigned_partitions: HashMap::new(),
        }
    }

    pub fn apply_system_event<F: FloEvent>(&mut self, event: &SystemEvent<F>) {
        let counter = event.counter();
        let term = event.term();

        match event.deserialized_data.kind {
            SystemEventKind::ClusterInitialized(ref initial_membership) => {
                for peer in initial_membership.peers.iter() {
                    self.add_peer(peer);
                }
            }
            SystemEventKind::NewClusterMemberJoining(ref new_peer) => {
                self.add_peer(new_peer);
            }
            _ => {}
        }
        self.last_applied = self.last_applied.max(counter);
        self.current_term = self.current_term.max(term);
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
    use event::{OwnedFloEvent, FloEventId, time};
    use test_utils::addr;

    #[test]
    fn applying_new_member_joining_adds_a_cluster_member() {
        let initial_peers = initial_peers();
        let mut subject = subject_with_initial_peers(initial_peers.clone());
        let new_peer = new_peer(4);
        let event = sys_event(2, 7, SystemEventKind::NewClusterMemberJoining(new_peer.clone()));
        subject.apply_system_event(&event);

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
        subject.apply_system_event(&event);
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

    fn sys_event(counter: EventCounter, term: Term, kind: SystemEventKind) -> SystemEvent<OwnedFloEvent> {
        let data = SystemEventData { term, kind };
        SystemEvent::new(FloEventId::new(0, counter), None, "/system".to_owned(), time::from_millis_since_epoch(44), data)
    }
}
