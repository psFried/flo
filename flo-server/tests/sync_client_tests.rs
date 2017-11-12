extern crate flo_client_lib;
extern crate url;
extern crate env_logger;
extern crate tempdir;

use std::fmt::Debug;
use std::time::Duration;
use std::net::{TcpStream, SocketAddr, SocketAddrV4, Ipv4Addr};

#[macro_use]
mod test_utils;

use test_utils::*;
use flo_client_lib::sync::{SyncConnection, EventIterator, EventToProduce};
use flo_client_lib::{FloEventId, Event, ErrorKind, VersionVector};
use flo_client_lib::codec::StringCodec;
use std::thread;

fn localhost(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port)
}

fn simple_event<N: Into<String>, D: Debug, E: Into<D>>(namespace: N, data: E) -> EventToProduce<D> {
    EventToProduce {
        partition: 1,
        namespace: namespace.into(),
        parent_id: None,
        data: data.into()
    }
}

fn test_with_server<F: FnOnce(u16)>(test_name: &'static str, flo_server_args: Vec<&str>, test_fun: F) {
    use tempdir::TempDir;

    let _ = ::env_logger::init();

    let (proc_type, port) = get_server_port();
    let temp_dir = TempDir::new(test_name).unwrap();
    let mut server_process = None;

    if proc_type == ServerProcessType::Child {
        let mut args = flo_server_args.iter().map(|a| a.to_string()).collect::<Vec<_>>();
        args.push("--use-new-engine".to_string());
        server_process = Some(FloServerProcess::with_args(port, temp_dir, args));
    }

    test_fun(port);

    if let Some(mut process) = server_process {
        process.kill();
    }
}

fn vv_from_start() -> VersionVector {
    let mut vv = VersionVector::new();
    vv.set(FloEventId::new(1, 0));
    vv
}

#[test]
fn produce_then_consume_one_large_event() {
    test_with_server("consumer_produces_and_consumes_a_large_event", Vec::new(), |port| {
        let mut connection = SyncConnection::connect(localhost(port), "large event", ::flo_client_lib::codec::RawCodec).expect("failed to connect");

        let event_size = 1024 * 1024;
        let event_data = ::std::iter::repeat(88u8).take(event_size).collect::<Vec<_>>();
        connection.produce(simple_event("/foo", event_data)).expect("failed to produce event");

        let vv = vv_from_start();

        let result = connection.into_consumer("/*", &vv, Some(1), false).next()
                .expect("Event was None!")
                .expect("Error reading event");

        assert_eq!(event_size, result.data.len());
    })
}

#[test]
fn produce_many_events_then_consume_them() {
    unimplemented!();
}

#[test]
fn consumer_receives_events_as_they_are_produced() {

    unimplemented!();
}

#[test]
fn consumer_completes_successfully_when_it_reaches_end_of_stream_even_when_event_limit_is_high() {

    unimplemented!();
}

