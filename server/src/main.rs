extern crate flo_server;
extern crate log4rs;
extern crate log;

mod logging;

use logging::init_logging;

fn main() {
    init_logging();
    flo_server::server::start_server();
}
