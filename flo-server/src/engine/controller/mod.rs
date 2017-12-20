pub mod cluster_state;
mod system_stream;
mod initialization;
mod controller_messages;
mod peer_connection;
mod tick_generator;

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
use self::cluster_state::{ClusterState, ConsensusProcessor};


pub use self::initialization::{start_controller, ControllerOptions, ClusterOptions};
pub use self::system_stream::SystemStreamRef;
pub use self::controller_messages::{SystemOperation, SystemOpType, ConnectionRef};
pub use self::cluster_state::{SharedClusterState, Peer, ClusterStateReader};

pub type SystemPartitionSender = ::std::sync::mpsc::Sender<SystemOperation>;
pub type SystemPartitionReceiver = ::std::sync::mpsc::Receiver<SystemOperation>;

pub fn create_system_partition_channels() -> (SystemPartitionSender, SystemPartitionReceiver) {
    ::std::sync::mpsc::channel()
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

    cluster_state: Box<ConsensusProcessor>,
}

impl FloController {
    pub fn new(system_partition: PartitionImpl,
               event_streams: HashMap<String, EventStreamRefMut>,
               storage_dir: PathBuf,
               cluster_state: Box<ConsensusProcessor>,
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
            cluster_state,
        }
    }

    fn process(&mut self, operation: SystemOperation) {
        warn!("Ignoring SystemOperation: {:?}", operation);

        //TODO: handle system operations
    }

    fn shutdown(&mut self) {
        info!("Shutting down FloController");
    }

    fn get_shared_streams(&self) -> Arc<Mutex<HashMap<String, EventStreamRef>>> {
        self.shared_event_stream_refs.clone()
    }
}



