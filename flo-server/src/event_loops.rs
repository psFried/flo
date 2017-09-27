use tokio_core::reactor::{Core, Remote};
use std::thread::{self, JoinHandle};

use std::cmp::{max, min};
use std::sync::mpsc::sync_channel;
use std::time::Duration;

fn thread_startup_timeout() -> Duration {
    Duration::from_millis(500)
}

fn spawn_event_loop_thread(thread_num: u8) -> Result<(JoinHandle<()>, Remote), String> {
    let (tx, rx) = sync_channel(0);

    let thread_handle = thread::Builder::new()
            .name(format!("client-io-event-loop-{}", thread_num))
            .spawn(move || {

                let mut reactor = Core::new().map_err(|err| {
                    format!("Failed to create event loop {} due to error: {:?}", thread_num, err)
                }).expect("Failed to create event loop Core");

                tx.send(reactor.remote()).expect("Failed to send remote back to calling thread");

                //Just start the loop without any work to do
                // it'll get work eventually from the remote
                loop {
                    reactor.turn(None);
                }
            }).map_err(|err| format!("Error starting thread for client i/o event loop {}: {:?}", thread_num, err));

    thread_handle.and_then(|join_handle| {
        rx.recv_timeout(thread_startup_timeout()).map_err(|err| {
            format!("Error starting IO event loop thread: {}", err)
        }).map(|remote| {
            (join_handle, remote)
        })
    })
}

pub struct EventLoopsJoinHandle(Vec<JoinHandle<()>>);
impl EventLoopsJoinHandle {
    pub fn join(self) {
        for handle in self.0 {
            match handle.join() {
                Ok(_) => {
                    // Should never enter here since there's a non-terminating loop polling the Core
                    unreachable!()
                }
                Err(_) => {
                    error!("Client I/O thread died unexpectedly")
                }
            }
        }
    }
}

fn get_default_io_thread_count() -> usize {
    let cpu_count = ::num_cpus::get();
    min(32, max(1, cpu_count.saturating_sub(2)))
}

fn get_io_thread_count(max_threads: Option<usize>) -> usize {
    let default = get_default_io_thread_count();
    max_threads.map(|n_threads| {
        min(default, n_threads)
    }).unwrap_or(default)
}

pub fn spawn_event_loop_threads(max: Option<usize>) -> Result<(EventLoopsJoinHandle, LoopHandles), String> {
    let num_threads = get_io_thread_count(max);
    info!("initializing {} client I/O threads", num_threads);
    let mut thread_handles = Vec::with_capacity(num_threads as usize);
    let mut remotes = Vec::with_capacity(num_threads as usize);

    for i in 0..num_threads {
        let (thread_handle, remote) = spawn_event_loop_thread(i as u8)?;
        thread_handles.push(thread_handle);
        remotes.push(remote);
    }

    Ok((EventLoopsJoinHandle(thread_handles), LoopHandles::new(remotes)))
}

#[derive(Clone)]
pub struct LoopHandles {
    handles: Vec<Remote>,
    current: usize,
}
impl LoopHandles {
    fn new(remotes: Vec<Remote>) -> LoopHandles {
        LoopHandles {
            handles: remotes,
            current: 0,
        }
    }

    pub fn next_handle(&mut self) -> Remote {
        let remote = self.handles[self.current].clone();
        self.current = (self.current + 1) % self.handles.len();
        remote
    }
}
