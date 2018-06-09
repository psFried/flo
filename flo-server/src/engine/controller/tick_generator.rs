use std::time::Duration;
use std::io;

use tokio_core::reactor::{Interval, Remote};
use futures::{Stream, Future};
use rand::distributions::{Range, IndependentSample};
use rand::thread_rng;

use engine::controller::SystemStreamRef;

#[derive(Debug)]
enum TickError {
    Io(io::Error),
    Send,
}

impl From<io::Error> for TickError {
    fn from(io_err: io::Error) -> Self {
        TickError::Io(io_err)
    }
}


pub fn spawn_tick_generator(tick_interval_millis: u64, remote: Remote, mut system_sender: SystemStreamRef) {
    // clone the inputs so we can re-run in the case of a failure
    let remote_copy = remote.clone();
    let mut system_stream_ref_clone = system_sender.clone();
    info!("Using tick interval of {} milliseconds", tick_interval_millis);

    remote.spawn(move |handle| {
        // TODO: handle the failure to create the Interval
        let interval = Interval::new(Duration::from_millis(tick_interval_millis), handle)
                .expect("failed to create tick interval");
        interval.map_err(|io_err| TickError::Io(io_err))
                .for_each(move |_| {
                    system_sender.tick().map_err(|_| TickError::Send)
                })
                .then(move |result| {
                    match result {
                        Ok(()) => unreachable!(),
                        Err(TickError::Send) => {
                            // system controller has shut down, so just quit
                            debug!("Shut down system tick generator")
                        }
                        Err(TickError::Io(io_err)) => {
                            error!("Error generating tick operations for system stream: {}", io_err);
                            system_stream_ref_clone.tick_error();
                            spawn_tick_generator(tick_interval_millis, remote_copy, system_stream_ref_clone);
                        }
                    }
                    // always return Ok so that this future will be considered resolved. We'll just spawn a new one anyway
                    Ok(())
                })
    });
}

pub fn get_election_timeout_millis() -> u64 {

    let range = Range::new(150u64, 300u64);
    range.ind_sample(&mut thread_rng())
}
