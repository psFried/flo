mod cluster_state;
mod system_stream;
mod initialization;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;
use std::net::SocketAddr;


use engine::event_stream::{EventStreamRef,
                               EventStreamRefMut,
                               EventStreamOptions};

use engine::event_stream::partition::Operation;
use engine::event_stream::partition::controller::PartitionImpl;
use atomics::AtomicBoolWriter;


pub use self::initialization::{start_controller, ControllerOptions, ClusterOptions};
pub use self::system_stream::SystemStreamRef;


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

    /// The address of the cluster's primary server, if one exists and it is known
    system_primary_server_addr: Arc<RwLock<Option<SocketAddr>>>,

    /// cluster parameters that this instance was started with. We'll almost certainly want to replace this field later on
    /// with something that can deal with more complexity
    cluster_options: Option<ClusterOptions>,
}

impl FloController {
    pub fn new(system_partition: PartitionImpl,
               system_primary_setter: AtomicBoolWriter,
               system_primary_address: Arc<RwLock<Option<SocketAddr>>>,
               event_streams: HashMap<String, EventStreamRefMut>,
               storage_dir: PathBuf,
               cluster_options: Option<ClusterOptions>,
               default_stream_options: EventStreamOptions) -> FloController {

        let stream_refs = event_streams.iter().map(|(k, v)| {
            (k.to_owned(), v.clone_ref())
        }).collect::<HashMap<String, EventStreamRef>>();

        FloController {
            shared_event_stream_refs: Arc::new(Mutex::new(stream_refs)),
            event_streams,
            system_partition,
            storage_dir,
            default_stream_options,
            system_primary_status_writer: system_primary_setter,
            system_primary_server_addr: system_primary_address,
            cluster_options,
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

    fn get_this_instance_address(&self) -> Option<SocketAddr> {
        self.cluster_options.as_ref().map(|opts| opts.this_instance_address)
    }
}



