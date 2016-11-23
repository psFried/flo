mod api;

use self::api::ClientMessage;
use std::sync::mpsc;
use std::thread;
use std::path::PathBuf;

pub fn run(storage_dir: PathBuf) -> mpsc::Sender<ClientMessage> {
    let (sender, receiver) = mpsc::channel::<ClientMessage>();

    //TODO: write this whole fucking thing
    thread::spawn(move || {

        loop {
            match receiver.recv() {
                Ok(msg) => info!("Received message: {:?}", msg),
                Err(recv_err) => {
                    error!("Receive Error: {:?}", recv_err);
                    break;
                }
            }
        }
    });

    sender
}
