extern crate url;
extern crate env_logger;
extern crate tempdir;
extern crate flo_client_lib;
extern crate flo_server;
extern crate futures;
extern crate tokio_core;
extern crate chrono;

#[macro_use]
extern crate log;

use tokio_core::reactor::Core;
use futures::{Stream, Future};
use chrono::Duration;

use flo_server::embedded::{EmbeddedFloServer, ControllerOptions, EventStreamOptions, run_embedded_server};
use flo_server::event_loops::{LoopHandles, spawn_event_loop_threads};

use flo_client_lib::{Event, VersionVector, FloEventId};
use flo_client_lib::async::AsyncClient;
use flo_client_lib::codec::{EventCodec, StringCodec};

fn default_test_options() -> EventStreamOptions {
    Default::default()
}

fn codec() -> Box<EventCodec<EventData=String>> {
    Box::new(StringCodec) as Box<EventCodec<EventData=String>>
}


fn integration_test<F>(test_name: &'static str, stream_opts: EventStreamOptions, fun: F) where F: Fn(EmbeddedFloServer, Core) {
    let _ = env_logger::init();
    println!("starting test: {}", test_name);

    let dir_name = test_name.replace("\\w", "-");
    let tmp_dir = tempdir::TempDir::new(&dir_name).expect("failed to create temp dir");

    let controller_options = ControllerOptions {
        storage_dir: tmp_dir.path().to_owned(),
        default_stream_options: stream_opts,
    };
    let embedded_server = run_embedded_server(controller_options).expect("failed to run embedded server");
    let reactor = Core::new().expect("failed to create reactor");

    fun(embedded_server, reactor);
}



#[test]
fn startup_and_shutdown_embedded_server() {
    integration_test("connect client", default_test_options(), |server, mut reactor| {
        let client = server.connect_client::<String>("testy mctesterson".to_owned(), codec());
        assert!(client.current_stream().is_none());
        let future = client.connect();
        let client = reactor.run(future).expect("failed to run connect");
        let stream_name: Option<&str> = client.current_stream().map(|s| s.name.as_ref());
        assert_eq!(Some("default"), stream_name)
    });
}


