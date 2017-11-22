mod system_stream;

use std::path::{PathBuf, Path};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::fs::DirEntry;
use std::io;

use tokio_core::reactor::Remote;

use engine::{EngineRef, system_stream_name, SYSTEM_STREAM_NAME};
use engine::event_stream::{EventStreamRef,
                               EventStreamRefMut,
                               EventStreamOptions,
                               init_existing_event_stream,
                               init_new_event_stream};

use engine::event_stream::partition::{PartitionSender,
                                      PartitionReceiver,
                                      PartitionRef,
                                      Operation,
                                      create_partition_channels};
use engine::event_stream::partition::controller::PartitionImpl;
use atomics::{AtomicBoolReader, AtomicBoolWriter, AtomicCounterReader};


pub use self::system_stream::SystemStreamRef;


/// Options passed to the controller on startup that determine how this instance will start and behave.
/// These options will come from the command line if this is a standalone server.
#[derive(Debug, PartialEq)]
pub struct ControllerOptions {
    pub storage_dir: PathBuf,
    pub default_stream_options: EventStreamOptions,
}


/// A specialized event stream that always has exactly one partition and manages the cluster state and consensus
/// Of course there is no cluster state and thus no consensus at the moment, but we'll just leave this here...
#[allow(dead_code)]
pub struct FloController {
    /// Shared references to all event streams in the system
    shared_event_stream_refs: Arc<Mutex<HashMap<String, EventStreamRef>>>,

    /// Unique mutable references to every event stream in the system
    event_streams: HashMap<String, EventStreamRefMut>,

    /// used as defaults when creating new event streams
    default_stream_options: EventStreamOptions,

    /// directory in which all event stream data is stored
    storage_dir: PathBuf,

    /// the partition that persists system events. Used as the RAFT log
    system_partition: PartitionImpl,

    /// used to set the status of the system stream. There is only ever at most one instance in a cluster
    /// where this variable is true ...if things actually work correctly ;)
    system_primary_status_writer: AtomicBoolWriter,
}

impl FloController {
    pub fn new(system_partition: PartitionImpl,
               system_primary_setter: AtomicBoolWriter,
               event_streams: HashMap<String, EventStreamRefMut>,
               storage_dir: PathBuf,
               default_stream_options: EventStreamOptions) -> FloController {
        let stream_refs = to_stream_refs(&event_streams);

        FloController {
            shared_event_stream_refs: Arc::new(Mutex::new(stream_refs)),
            event_streams,
            system_partition,
            storage_dir,
            default_stream_options,
            system_primary_status_writer: system_primary_setter,
        }
    }

    fn process(&mut self, _operation: Operation) {
        unimplemented!()
    }

    fn shutdown(&mut self) {
        info!("Shutting down FloController");
    }

    fn get_shared_streams(&self) -> Arc<Mutex<HashMap<String, EventStreamRef>>> {
        self.shared_event_stream_refs.clone()
    }
}

fn to_stream_refs(mut_refs: &HashMap<String, EventStreamRefMut>) -> HashMap<String, EventStreamRef> {
    mut_refs.iter().map(|(k, v)| {
        (k.to_owned(), v.clone_ref())
    }).collect::<HashMap<String, EventStreamRef>>()
}


pub fn start_controller(options: ControllerOptions, remote: Remote) -> io::Result<EngineRef> {
    debug!("Starting Flo Controller with: {:?}", options);
    let ControllerOptions{storage_dir, default_stream_options} = options;

    // TODO: initialize system primary status to false once clustering works
    let system_primary_writer = AtomicBoolWriter::with_value(true);
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
                                                    user_streams,
                                                    storage_dir,
                                                    default_stream_options);

            let (system_partition_tx, system_partition_rx) = create_partition_channels();
            let engine_ref = create_engine_ref(&flo_controller, system_highest_counter, system_primary_reader, system_partition_tx);

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
                     system_sender: PartitionSender) -> EngineRef {

    let system_partition_ref = PartitionRef::new(system_stream_name(),
                                                 1,
                                                 system_highest_counter,
                                                 system_primary_reader,
                                                 system_sender);

    let system_stream_ref = SystemStreamRef::new(system_partition_ref);
    let shared_stream_refs = controller.get_shared_streams();
    EngineRef::new(system_stream_ref, shared_stream_refs)
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

