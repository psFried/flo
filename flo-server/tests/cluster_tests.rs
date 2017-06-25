extern crate flo_client_lib;
extern crate url;
extern crate env_logger;
extern crate tempdir;

mod test_utils;

use test_utils::*;
use flo_client_lib::sync::connection::{Connection, ConsumerOptions};
use flo_client_lib::sync::{Consumer, Context, ConsumerAction, ClientError};
use flo_client_lib::{FloEventId, ActorId, Event, ErrorKind};
use flo_client_lib::codec::StringCodec;
use std::thread;
use std::time::Duration;
use std::sync::atomic::Ordering;
use std::net::{TcpStream, SocketAddr, SocketAddrV4, Ipv4Addr};
use tempdir::TempDir;

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port))
}


#[derive(Debug)]
struct ClusterMember {
    port: u16,
    actor: ActorId,
    process_type: ServerProcessType,
    process: Option<FloServerProcess>
}

impl ClusterMember {

    fn new(actor_id: ActorId) -> ClusterMember {
        let (proc_type, port) = get_cluster_member_port(actor_id);
        ClusterMember {
            port: port,
            actor: actor_id,
            process_type: proc_type,
            process: None,
        }
    }

    fn address(&self) -> SocketAddr {
        localhost(self.port)
    }

    fn start(&mut self, peer_ports: Vec<u16>) {
        if self.process.is_some() || self.process_type == ServerProcessType::Detached {
            return;
        }

        let data_dir = TempDir::new("flo-cluster-test").unwrap();
        let process = FloServerProcess::clustered(self.port, self.actor, data_dir, &peer_ports, Vec::new());
        self.process = Some(process);
    }

    fn new_connection(&self) -> Connection<String, StringCodec> {
        let addy = self.address();
        Connection::connect(addy, StringCodec).expect("Failed to create connection")
    }

}

fn get_cluster_member_port(actor: u16) -> (ServerProcessType, u16) {
    let var_name = format!("FLO_TEST_PORT_{}", actor);
    ::std::env::var(var_name).ok().map(|value| {
        (ServerProcessType::Detached, value.parse::<u16>().unwrap())
    }).unwrap_or_else(|| {
        unsafe {
            (ServerProcessType::Child, 3001u16 + PORT.fetch_add(1, Ordering::SeqCst) as u16)
        }
    })
}

fn start_cluster(member_count: u16) -> Vec<ClusterMember> {
    let mut members: Vec<ClusterMember> = (0..member_count).map(|i| {
        let actor_id = i + 1;
        ClusterMember::new(actor_id)
    }).collect();

    let mut port_args = members.iter().map(|member| {
        members.iter().filter(|m| m.actor != member.actor).map(|m| m.port).collect::<Vec<u16>>()
    }).collect::<Vec<Vec<u16>>>();

    for (member, peer_ports) in members.iter_mut().zip(port_args.drain(..)) {
        member.start(peer_ports)
    }
    members
}

fn start_3_member_cluster() -> (ClusterMember, ClusterMember, ClusterMember) {
    let mut members = start_cluster(3);
    assert_eq!(3, members.len());
    let three = members.pop().unwrap();
    let two = members.pop().unwrap();
    let one = members.pop().unwrap();
    (one, two, three)
}


#[test]
fn basic_replication_test() {
    let (mut server_one, mut server_two, mut server_three) = start_3_member_cluster();

    let mut client_one = server_one.new_connection();
    let mut client_two = server_two.new_connection();
    let mut client_three = server_three.new_connection();

    // shouldn't matter if these events all have the same counter since we're producing them in order of actor precedence
    let event_one_id = client_one.produce("/foo/bar", "some event data").expect("failed to produce event for actor 1");
    let event_two_id = client_two.produce("/foo/bar", "some event data").expect("failed to produce event for actor 2");
    let event_three_id = client_three.produce("/foo/bar", "some event data").expect("failed to produce event for actor 3");

    thread::sleep(Duration::from_millis(250));
    let mut expected_ids = vec![event_one_id, event_two_id, event_three_id];
    expected_ids.sort();

    assert_server_events_equal(&mut client_one, &expected_ids);
    assert_server_events_equal(&mut client_two, &expected_ids);
    assert_server_events_equal(&mut client_three, &expected_ids);
}

fn assert_server_events_equal(connection: &mut Connection<String, StringCodec>, expected_event_ids: &Vec<FloEventId>) {
    use std::time::Instant;

    let timeout = Duration::from_millis(1000);
    let start_time = Instant::now();

    let mut actual_events = get_all_event_ids(connection);

    while (start_time.elapsed() < timeout) {
        if actual_events.len() >= expected_event_ids.len() {
            assert_eq!(expected_event_ids, &actual_events);
            return;
        } else {
            thread::sleep(Duration::from_millis(100));
            actual_events = get_all_event_ids(connection);
        }
    }
    panic!("Expected event ids: {:?}, actual: {:?}", expected_event_ids, actual_events);
}

fn get_all_event_ids(connection: &mut Connection<String, StringCodec>) -> Vec<FloEventId> {
    connection.iter(ConsumerOptions::default())
            .expect("failed to create iterator")
            .map(|event_result| {
                event_result.expect("failed to read next event").id
            }).collect()
}
