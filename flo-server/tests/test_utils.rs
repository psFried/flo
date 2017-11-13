#![allow(dead_code)] // will generate a ton of bogus warnings otherwise


extern crate url;
extern crate env_logger;
extern crate tempdir;
extern crate flo_client_lib;

use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use std::sync::{Once, ONCE_INIT};

#[allow(unused_imports)] //Ordering will say it's unused, because it's only used in the macro
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

use tempdir::TempDir;


pub static mut PORT: AtomicUsize = ATOMIC_USIZE_INIT;

static ON_START: Once = ONCE_INIT;

pub fn init_logger() {
    ON_START.call_once(|| {
        let _ = env_logger::init();
    });
}

#[derive(PartialEq, Debug)]
pub enum ServerProcessType {
    Detached,
    Child
}

pub fn get_server_port() -> (ServerProcessType, u16) {
    ::std::env::var("FLO_TEST_PORT").ok().map(|value| {
        (ServerProcessType::Detached, value.parse::<u16>().unwrap())
    }).unwrap_or_else(|| {
        unsafe {
            (ServerProcessType::Child, 3001u16 + PORT.fetch_add(1, Ordering::SeqCst) as u16)
        }
    })
}

#[derive(Debug)]
pub struct FloServerProcess {
    child_proc: Option<Child>,
    port: u16,
    data_dir: TempDir,
    args: Vec<String>,
}

impl FloServerProcess {
    pub fn new(port: u16, data_dir: TempDir) -> FloServerProcess {
        FloServerProcess::with_args(port, data_dir, Vec::new())
    }

    pub fn clustered(my_port: u16, my_actor: u16, data_dir: TempDir, peer_ports: &Vec<u16>, mut additional_args: Vec<String>) -> FloServerProcess {
        additional_args.push("-A".to_owned());
        additional_args.push(my_actor.to_string());

        for port in peer_ports {
            let address = format!("127.0.0.1:{}", port);
            additional_args.push("-P".to_owned());
            additional_args.push(address);
        }

        FloServerProcess::with_args(my_port, data_dir, additional_args)
    }

    pub fn with_args(port: u16, data_dir: TempDir, args: Vec<String>) -> FloServerProcess {
        let mut server_proc = FloServerProcess {
            child_proc: None,
            port: port,
            data_dir: data_dir,
            args: args,
        };
        server_proc.start();
        server_proc
    }

    pub fn start(&mut self) {
        use std::env::current_dir;
        use std::process::Stdio;

        assert!(self.child_proc.is_none(), "tried to start server but it's already started");

        let mut flo_path = current_dir().unwrap();
        flo_path.push("../target/debug/flo");

        println!("Starting flo server");
        let child = Command::new(flo_path)
                .env("RUST_BACKTRACE", "1")
                .arg("--port")
                .arg(format!("{}", self.port))
                .arg("--max-io-threads")
                .arg("2")
                .arg("--data-dir")
                .arg(self.data_dir.path().to_str().unwrap())
                .args(&self.args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn().unwrap();
        self.child_proc = Some(child);

        // hack to prevent test from starting until the server is actually started
        // TODO: wait for log output that indicates server is ready
        thread::sleep(Duration::from_millis(1000));
    }

    pub fn kill(&mut self) {
        self.child_proc.take().map(|mut child| {
            println!("killing flo server proc");
            child.kill().unwrap();
            child.wait_with_output().map(|output| {
                let stdout = String::from_utf8_lossy(&output.stdout);
                println!("stdout: \n {}", stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                println!("stderr: \n {}", stderr);
            }).unwrap();
            println!("flo server proc completed");
        });
    }
}

impl Drop for FloServerProcess {
    fn drop(&mut self) {
        self.kill();
    }
}
