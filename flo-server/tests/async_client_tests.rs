
extern crate flo_server;
extern crate flo_client_lib;
extern crate url;
extern crate env_logger;
extern crate tempdir;
extern crate tokio_core;
extern crate futures;

mod test_utils;

use std::net::{SocketAddr, SocketAddrV4};

use tokio_core::reactor::{Core, Handle};
use futures::{Future, Stream};
use tempdir::TempDir;

use flo_client_lib::async::{AsyncClient, tcp_connect};
use flo_client_lib::codec::StringCodec;
use flo_client_lib::{FloEventId, VersionVector};
use test_utils::*;

fn localhost(port: u16) -> SocketAddr {
    ([127, 0, 0, 1], port).into()
}


fn run_test<F>(desc: &'static str, test_fun: F) where F: Fn(u16) + std::panic::RefUnwindSafe {
    init_logger();
    let (proc_type, port) = get_server_port();
    let server_stuff = if proc_type == ServerProcessType::Child {
        let temp_dir = TempDir::new(desc).unwrap();
        let args = vec!["--use-new-engine".to_string()];
        let mut server_proc = FloServerProcess::with_args(port, temp_dir, args);
        Some(server_proc)
    } else {
        None
    };

    let result = ::std::panic::catch_unwind(|| {
        test_fun(port);
    });
    if let Some(mut server) = server_stuff {
        server.kill();
    }
    result.expect("Test failed");
}


#[test]
fn produce_one_event_and_read_it_back() {
    let desc = "basic_produce_and_consume";
    run_test(desc, |port| {
        let mut core = Core::new().unwrap();
        let connect = tcp_connect(desc, &localhost(port), StringCodec, &core.handle());
        let client: AsyncClient<String> = core.run(connect).expect("failed to connect client");

        let produce = client.produce("/foo/bar", None, "some event data".to_owned());
        let (id, client) = core.run(produce).expect("failed to produce event");
        assert_eq!(FloEventId::new(1, 1), id);

        let mut vv = VersionVector::new();
        vv.set(FloEventId::new(1, 0));
        let stream = client.consume("/foo/*", &vv, Some(1));
        let mut collected = core.run(stream.collect()).expect("failed to consume");
        assert_eq!(1, collected.len());
        let event = collected.pop().unwrap();
        assert_eq!(FloEventId::new(1, 1), event.id);
        assert_eq!("some event data", &event.data);
    })
}


