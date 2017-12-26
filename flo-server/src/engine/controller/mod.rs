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


use engine::ConnectionId;
use engine::event_stream::{EventStreamRef,
                               EventStreamRefMut,
                               EventStreamOptions};

use engine::event_stream::partition::Operation;
use engine::event_stream::partition::controller::PartitionImpl;
use atomics::AtomicBoolWriter;
use self::cluster_state::{ClusterManager, ConsensusProcessor};


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

    all_connections: HashMap<ConnectionId, ConnectionRef>,
}

impl FloController {
    pub fn new(system_partition: PartitionImpl,
               event_streams: HashMap<String, EventStreamRefMut>,
               shared_stream_refs: Arc<Mutex<HashMap<String, EventStreamRef>>>,
               storage_dir: PathBuf,
               cluster_state: Box<ConsensusProcessor>,
               default_stream_options: EventStreamOptions) -> FloController {

        FloController {
            shared_event_stream_refs: shared_stream_refs,
            event_streams,
            system_partition,
            storage_dir,
            default_stream_options,
            cluster_state,
            all_connections: HashMap::with_capacity(4),
        }
    }

    fn process(&mut self, operation: SystemOperation) {

        let SystemOperation {connection_id, op_start_time, op_type } = operation;
        trace!("Received system op: {:?}", op_type);
        // TODO: time operation handling and record perf metrics

        match op_type {
            SystemOpType::IncomingConnectionEstablished(connection_ref) => {
                self.all_connections.insert(connection_id, connection_ref);
            }
            SystemOpType::OutgoingConnectionFailed(address) => {
                self.cluster_state.outgoing_connection_failed(address);
            }
            SystemOpType::ConnectionUpgradeToPeer(peer_id) => {
                if let Some(connection_ref) = self.all_connections.get(&connection_id).cloned() {
                    unimplemented!()
                } else {
                    error!("Got ConnectionUpgradeToPeer for connection_id: {}, but no connection with that id exists", connection_id);
                }
            }
            other @ _ => {
                warn!("Ignoring SystemOperation: {:?}", other);
            }
        }
    }

    fn shutdown(&mut self) {
        info!("Shutting down FloController");
    }

}



