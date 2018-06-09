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
use std::time::Instant;

use protocol::{ProduceEvent, Term};
use event::EventCounter;
use engine::ConnectionId;
use engine::event_stream::{EventStreamRef,
                               EventStreamRefMut,
                               EventStreamOptions};
use engine::event_stream::partition::{self, PersistentEvent, IndexEntry, SegmentNum};
use engine::event_stream::partition::controller::PartitionImpl;
use self::cluster_state::ConsensusProcessor;


pub use self::initialization::{start_controller, ControllerOptions, ClusterOptions};
pub use self::system_stream::SystemStreamRef;
pub use self::system_event::{SystemEvent, SystemEventData, SystemEventKind, QualifiedPartitionId};
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


/// A specialized event stream that always has exactly one partition and manages the cluster state and consensus processor
/// The `ConsensusProcessor` manages most operations on the `ControllerState`. If this instance is running in standalone mode
/// then all operations will basically just immediately succeed
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
                cluster_state.connection_closed(connection_id);
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
                handle_partition_op(connection_id, op_start_time, partition_op, cluster_state.as_mut(), controller_state);
            }
        }
    }

    fn shutdown(&mut self) {
        info!("Shutting down FloController");
        //TODO: either do something on shutdown or delete this function
    }

}


fn handle_partition_op(connection_id: ConnectionId, op_start_time: Instant, op: partition::OpType,
                       cluster_state: &mut ConsensusProcessor, controller_state: &mut ControllerStateImpl) {
    use engine::event_stream::partition::OpType::*;
    match op {
        Produce(produce_op) => {
            produce_system_events(produce_op, cluster_state, controller_state);
        }
        Consume(consume_op) => {
            // `handle_consume` always just returns `Ok(())` at the moment anyway. Should probably just change it to unit return type
            let _ = controller_state.system_partition.handle_consume(connection_id, consume_op);
        }
        StopConsumer => {
            controller_state.system_partition.stop_consumer(connection_id);
        }
        Replicate(_) => {
            error!("Received normal Replicate operation from connection_id: {} in system partition", connection_id);
        }
        Tick => {
            // only used by the partition to expire old events, which we do not do for the system partition.
            // anyway, nothing is creating these operations anyway
        }
    }
}

fn produce_system_events(mut produce_op: partition::ProduceOperation, cluster_state: &mut ConsensusProcessor, controller_state: &mut ControllerStateImpl) {
    if cluster_state.is_primary() {
        let term = cluster_state.get_current_term();
        let validate_result = validate_system_event(&mut produce_op.events, term);

        if let Err(err) = validate_result {
            // We're done here
            produce_op.client.complete(Err(err));
        } else {
            // hand off the modified operation to the partition, which will complete it
            let result = controller_state.system_partition.handle_produce(produce_op);
            if let Err(partition_err) = result {
                error!("Partition error creating new system events: {:?}", partition_err);
            } else {
                // success! send out AppendEntries
                cluster_state.send_append_entries(controller_state)
            }
        }
    } else {
        let err = io::Error::new(io::ErrorKind::Other, "Not primary");
        produce_op.client.complete(Err(err));
    }
}


fn validate_system_event(events: &Vec<ProduceEvent>, term: Term) -> io::Result<()> {
    for event in events.iter() {
        if event.data.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Event has no body"));
        }
    }
    Ok(())
}


