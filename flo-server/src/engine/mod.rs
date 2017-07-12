pub mod api;
pub mod event_store;

mod producer;
mod consumer;

use self::api::{ConsumerManagerMessage, ProducerManagerMessage};
use self::producer::ProducerManager;
use self::consumer::ConsumerManager;
use server::ServerOptions;
use engine::event_store::{StorageEngine, StorageEngineOptions};
use engine::event_store::fs::{FSStorageEngine};

use futures::sync::mpsc::UnboundedSender;

use std::net::SocketAddr;
use std::sync::mpsc;
use std::thread;

pub use self::producer::tick_duration;

pub struct BackendChannels {
    pub producer_manager: mpsc::Sender<ProducerManagerMessage>,
    pub consumer_manager: mpsc::Sender<ConsumerManagerMessage>,
}

//TODO: return a result from `run` instead of panicking
pub fn run(options: ServerOptions, cluster_sender: UnboundedSender<SocketAddr>) -> BackendChannels {
    let (producer_tx, producer_rx) = mpsc::channel::<ProducerManagerMessage>();
    let (consumer_tx, consumer_rx) = mpsc::channel::<ConsumerManagerMessage>();

    let ServerOptions{port,
        data_dir,
        event_retention_duration,
        event_eviction_period,
        max_cache_memory,
        cluster_addresses,
        actor_id, ..} = options;

    let storage_options = StorageEngineOptions {
        storage_dir: data_dir,
        event_eviction_period: event_eviction_period,
        event_retention_duration: event_retention_duration
    };

    let (event_writer, event_reader, version_vec) = FSStorageEngine::initialize(storage_options).expect("Failed to initialize storage engine");
    let highest_event_id = version_vec.max();
    info!("initialized storage engine with highest event id: {}, version vec: {:?}", highest_event_id, version_vec);

    // Initialize Producer Manager first
    let expiration_check_period = event_eviction_period.to_std().expect("Invalid event eviction check period");

    let consumer_manager_sender = consumer_tx.clone();
    thread::Builder::new().name("Producer-Manager-thread".to_owned()).spawn(move || {
        let peer_addresses = cluster_addresses.unwrap_or(Vec::new());

        let mut producer_manager = ProducerManager::new(event_writer,
                                                        consumer_manager_sender,
                                                        actor_id,
                                                        port,
                                                        version_vec,
                                                        expiration_check_period,
                                                        peer_addresses,
                                                        cluster_sender);
        loop {
            match producer_rx.recv() {
                Ok(msg) => {
                    match producer_manager.process(msg) {
                        Ok(()) => {
//                            trace!("Producer Manager successfully processed message");
                        }
                        Err(err) => {
                            error!("ProducerManager error processing message err: {:?}", err);
                        }
                    }
                }
                Err(recv_err) => {
                    error!("Receive Error: {:?}\nShutting down ProducerManager", recv_err);
                    break;
                }
            }
        }
    }).expect("Failed to start Producer Manager thread");

    // Initialize Consumer Manager
    let consumer_manager_sender = consumer_tx.clone();
    thread::Builder::new().name("Consumer-Manager-thread".to_owned()).spawn(move || {
        let mut consumer_manager = ConsumerManager::new(event_reader, consumer_manager_sender, highest_event_id, event_retention_duration, max_cache_memory);

        loop {
            match consumer_rx.recv() {
                Ok(client_message) => {
                    match consumer_manager.process(client_message) {
                        Ok(()) => {
//                            trace!("Consumer manager succesfully processed message");
                        }
                        Err(err) => {
                            error!("ConsumerManager error in processing message err: {:?}", err);
                        }
                    }
                }
                Err(err) => {
                    error!("Receive Error: {:?}\nshutting down Consumer Manager", err);
                    break;
                }
            }
        }
    }).expect("Failed to start producer manager thread");

    BackendChannels {
        producer_manager: producer_tx,
        consumer_manager: consumer_tx.clone(),
    }
}

