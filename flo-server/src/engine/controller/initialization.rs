use std::sync::{Arc, RwLock};
use std::path::{PathBuf, Path};
use std::collections::HashMap;
use std::fs::DirEntry;
use std::io;
use std::net::SocketAddr;

use tokio_core::reactor::Remote;

use engine::{EngineRef, system_stream_name, SYSTEM_STREAM_NAME};
use engine::event_stream::{EventStreamRefMut,
                           EventStreamOptions,
                           init_existing_event_stream,
                           init_new_event_stream};

use engine::event_stream::partition::{PartitionSender,
                                      PartitionReceiver,
                                      PartitionRef,
                                      create_partition_channels};
use engine::event_stream::partition::controller::PartitionImpl;
use atomics::{AtomicBoolReader, AtomicBoolWriter, AtomicCounterReader};

use super::{FloController, SystemStreamRef};


/// Options passed to the controller on startup that determine how this instance will start and behave.
/// These options will come from the command line if this is a standalone server.
#[derive(Debug, PartialEq)]
pub struct ControllerOptions {
    pub storage_dir: PathBuf,
    pub default_stream_options: EventStreamOptions,
    pub cluster_options: Option<ClusterOptions>,
}

#[derive(Debug, PartialEq)]
pub struct ClusterOptions {
    pub this_instance_address: SocketAddr,
    pub peer_addresses: Vec<SocketAddr>,
}

pub fn start_controller(options: ControllerOptions, remote: Remote) -> io::Result<EngineRef> {
    debug!("Starting Flo Controller with: {:?}", options);
    let ControllerOptions{storage_dir, default_stream_options, cluster_options} = options;

    // TODO: initialize system primary status to false once clustering works
    let system_primary_writer = AtomicBoolWriter::with_value(true);
    let system_primary_server_addr = Arc::new(RwLock::new(None));

    let partition_result = init_system_partition(&storage_dir,
                                                 system_primary_writer.reader(),
                                                 &default_stream_options);

    partition_result.and_then(|system_partition| {
        debug!("Initialized system partition");
        init_user_streams(&storage_dir, &default_stream_options, &remote).map(|user_streams| {
            debug!("Initialized all {} user event streams", user_streams.len());

            let system_primary_reader = system_primary_writer.reader();
            let system_highest_counter = system_partition.event_counter_reader();

            let flo_controller = FloController::new(system_partition,
                                                    system_primary_writer,
                                                    system_primary_server_addr.clone(),
                                                    user_streams,
                                                    storage_dir,
                                                    cluster_options,
                                                    default_stream_options);

            let (system_partition_tx, system_partition_rx) = create_partition_channels();
            let engine_ref = create_engine_ref(&flo_controller, system_highest_counter, system_primary_reader, system_primary_server_addr, system_partition_tx);

            run_controller_impl(flo_controller, system_partition_rx);
            engine_ref
        })
    })
}

fn init_system_partition(storage_dir: &Path, system_primary_reader: AtomicBoolReader, default_stream_options: &EventStreamOptions) -> io::Result<PartitionImpl> {
    use engine::event_stream::HighestCounter;

    let mut system_partition_dir: PathBuf = storage_dir.join(system_stream_name());
    system_partition_dir.push("1");

    if system_partition_dir.is_dir() {
        PartitionImpl::init_existing(1,
                                     system_partition_dir,
                                     default_stream_options,
                                     system_primary_reader,
                                     HighestCounter::zero())
    } else {
        PartitionImpl::init_new(1,
                                system_partition_dir,
                                default_stream_options,
                                system_primary_reader,
                                HighestCounter::zero())
    }
}

fn create_engine_ref(controller: &FloController,
                     system_highest_counter: AtomicCounterReader,
                     system_primary_reader: AtomicBoolReader,
                     system_primary_addr: Arc<RwLock<Option<SocketAddr>>>,
                     system_sender: PartitionSender) -> EngineRef {

    let system_partition_ref = PartitionRef::new(system_stream_name(),
                                                 1,
                                                 system_highest_counter,
                                                 system_primary_reader,
                                                 system_sender,
                                                 system_primary_addr);

    let system_stream_ref = SystemStreamRef::new(system_partition_ref);
    let shared_stream_refs = controller.get_shared_streams();
    let this_addr = controller.get_this_instance_address();
    EngineRef::new(this_addr, system_stream_ref, shared_stream_refs)
}


fn init_user_streams(storage_dir: &Path, options: &EventStreamOptions, remote: &Remote) -> io::Result<HashMap<String, EventStreamRefMut>> {
    let mut user_streams = HashMap::new();
    for file_result in ::std::fs::read_dir(storage_dir)? {
        let entry = file_result?;
        if is_user_event_stream(&entry)? {
            let stream_storage = entry.path();
            debug!("attempting to initialize user stream at path: {:?}", stream_storage);
            let stream = init_existing_event_stream(
                stream_storage,
                options.clone(),
                remote.clone())?;
            user_streams.insert(stream.get_name().to_owned(), stream);
        }
    }

    if !user_streams.contains_key(&options.name) {
        let new_stream_dir = storage_dir.join(&options.name);
        let new_stream = init_new_event_stream(
            new_stream_dir,
            options.clone(),
            remote.clone())?;

        user_streams.insert(options.name.clone(), new_stream);
    }

    Ok(user_streams)
}


fn is_user_event_stream(dir_entry: &DirEntry) -> io::Result<bool> {
    let is_dir = dir_entry.file_type()?.is_dir();
    Ok(is_dir && SYSTEM_STREAM_NAME != &dir_entry.file_name())
}


fn run_controller_impl(mut controller: FloController, system_partition_rx: PartitionReceiver) {
    ::std::thread::spawn(move || {
        debug!("Starting FloController processing");
        while let Ok(operation) = system_partition_rx.recv() {
            controller.process(operation);
        }
        controller.shutdown();
    });
}
