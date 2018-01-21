pub mod cluster_state;
pub mod tick_generator;
pub mod system_event;
mod system_stream;
mod initialization;
mod controller_messages;
mod peer_connection;
mod system_reader;
mod controller_state;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;
use std::io;

use event::EventCounter;
use engine::ConnectionId;
use engine::event_stream::{EventStreamRef,
                               EventStreamRefMut,
                               EventStreamOptions};
use engine::event_stream::partition::{PersistentEvent, IndexEntry, SegmentNum};
use engine::event_stream::partition::controller::PartitionImpl;
use self::cluster_state::ConsensusProcessor;


pub use self::initialization::{start_controller, ControllerOptions, ClusterOptions};
pub use self::system_stream::SystemStreamRef;
pub use self::system_event::{SystemEvent, SystemEventData};
pub use self::system_reader::{SystemStreamReader, SYSTEM_READER_BATCH_SIZE};
pub use self::controller_messages::*;
pub use self::cluster_state::{SharedClusterState, ClusterStateReader};
pub use self::controller_state::{ControllerState, ControllerStateImpl};

#[cfg(test)]
pub use self::controller_state::mock;

pub type SystemPartitionSender = ::std::sync::mpsc::Sender<SystemOperation>;
pub type SystemPartitionReceiver = ::std::sync::mpsc::Receiver<SystemOperation>;

pub fn create_system_partition_channels() -> (SystemPartitionSender, SystemPartitionReceiver) {
    ::std::sync::mpsc::channel()
}



/// A specialized event stream that always has exactly one partition and manages the cluster state and consensus
/// Of course there is no cluster state and thus no consensus at the moment, but we'll just leave this here...
#[allow(dead_code)]
pub struct FloController {
    controller_state: ControllerStateImpl,
    cluster_state: Box<ConsensusProcessor>,
}

impl FloController {
    pub fn new(system_partition: PartitionImpl,
               event_streams: HashMap<String, EventStreamRefMut>,
               shared_stream_refs: Arc<Mutex<HashMap<String, EventStreamRef>>>,
               storage_dir: PathBuf,
               cluster_state: Box<ConsensusProcessor>,
               default_stream_options: EventStreamOptions) -> FloController {

        let controller_state = ControllerStateImpl::new(system_partition, event_streams, shared_stream_refs,
                                                        storage_dir, default_stream_options);

        FloController {
            cluster_state,
            controller_state,
        }
    }

    fn process(&mut self, operation: SystemOperation) {

        let SystemOperation {connection_id, op_start_time, op_type } = operation;

        if !op_type.is_tick() {
            trace!("Received system op: {:?}", op_type);
        }
        // TODO: time operation handling and record perf metrics

        let FloController{ref mut cluster_state, ref mut controller_state, ..} = *self;
        match op_type {
            SystemOpType::IncomingConnectionEstablished(connection_ref) => {
                controller_state.all_connections.insert(connection_id, connection_ref);
            }
            SystemOpType::ConnectionClosed => {
                // TODO: do we need to inform ConsensusProcessor about the connection in case it is a peer connection?
                controller_state.all_connections.remove(&connection_id);
            }
            SystemOpType::OutgoingConnectionFailed(address) => {
                cluster_state.outgoing_connection_failed(connection_id, address);
                controller_state.all_connections.remove(&connection_id);
            }
            SystemOpType::ConnectionUpgradeToPeer(upgrade) => {
                cluster_state.peer_connection_established(upgrade, connection_id, controller_state);
            }
            SystemOpType::Tick => {
                cluster_state.tick(op_start_time, controller_state);
            }
            SystemOpType::RequestVote(request) => {
                cluster_state.request_vote_received(connection_id, request);
            }
            SystemOpType::VoteResponseReceived(response) => {
                cluster_state.vote_response_received(op_start_time, connection_id, response, controller_state);
            }
            SystemOpType::AppendEntriesReceived(append) => {
                cluster_state.append_entries_received(connection_id, append, controller_state);
            }
            SystemOpType::AppendEntriesResponseReceived(response) => {
                cluster_state.append_entries_response_received(connection_id, response, controller_state);
            }
            SystemOpType::PartitionOp(partition_op) => {
                warn!("Ignoring PartitionOp: {:?}", partition_op);
            }
        }
    }

    fn shutdown(&mut self) {
        info!("Shutting down FloController");
        //TODO: either do something on shutdown or delete this function
    }

}




