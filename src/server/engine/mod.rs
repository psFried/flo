pub mod api;

mod producer;
mod consumer;
mod client_map;

use self::api::ClientMessage;
use self::producer::ProducerManager;
use self::consumer::ConsumerManager;
use event_store::test_util::{TestStorageEngine, TestEventWriter};
use event_store::{StorageEngine, EventWriter, EventReader, StorageEngineOptions};
use flo_event::ActorId;

use futures::sync::mpsc::UnboundedSender;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::path::PathBuf;
use std::net::SocketAddr;

pub struct BackendChannels {
    pub producer_manager: mpsc::Sender<ClientMessage>,
    pub consumer_manager: mpsc::Sender<ClientMessage>,
}

pub fn run(storage_options: StorageEngineOptions) -> BackendChannels {
    let (storage_sender, storage_receiver) = mpsc::channel::<ClientMessage>();
    let (reader_sender, reader_receiver) = mpsc::channel::<ClientMessage>();

    //TODO: set max events and namespace and have some proper error handling
    let actor_id: ActorId = 1;
    let (mut event_writer, mut event_reader) = TestStorageEngine::initialize(storage_options).unwrap();
    let highest_event_id = event_reader.get_highest_event_id();


    //TODO: write this whole fucking thing
    let consumer_manager_sender = reader_sender.clone();
    thread::spawn(move || {
        let mut producer_manager = ProducerManager::new(event_writer, consumer_manager_sender, actor_id, highest_event_id.event_counter);
        loop {
            match storage_receiver.recv() {
                Ok(msg) => {
                    producer_manager.process(msg).unwrap();
                }
                Err(recv_err) => {
                    error!("Receive Error: {:?}", recv_err);
                    break;
                }
            }
        }
    });

    let consumer_manager_sender = reader_sender.clone();
    thread::spawn(move || {
        let mut consumer_manager = ConsumerManager::new(event_reader, consumer_manager_sender);

        loop {
            match reader_receiver.recv() {
                Ok(client_message) => {
                    consumer_manager.process(client_message).unwrap();
                }
                Err(err) => {
                    error!("Error reading for Consumer Manager: {:?}", err);
                    break;
                }
            }
        }
    });

    BackendChannels {
        producer_manager: storage_sender,
        consumer_manager: reader_sender.clone(),
    }
}

