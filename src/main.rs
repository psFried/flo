extern crate flo;
extern crate log4rs;
extern crate log;

mod logging;

use logging::init_logging;

fn main() {
    init_logging();
    flo::server::start_server();
}
