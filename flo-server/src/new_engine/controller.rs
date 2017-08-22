
use std::sync::Arc;
use std::collections::HashMap;

use new_engine::api::{PartitionMessage,
                      PartitionOperation,
                      EngineRef,
                      EventStreamRef,
                      PartitionRef,
                      EngineSender,
                      EngineReceiver,
                      ClientSender,
                      PartitionSendError,
                      EventStreamOptions};


pub struct ControllerOptions {
    automatically_create_streams: bool,
    default_stream_options: EventStreamOptions,
}


/// A specialized event stream that always has exactly one partition and manages the cluster state and consensus
/// Of course there is no cluster state and thus no consensus at the moment, but we'll just leave this here...
pub struct FloController { // TODO: implement raft
    engine_ref: Arc<EngineRef>,
    event_streams: HashMap<String, EventStreamRef>,
}


pub fn start_controller() -> Arc<EngineRef> {
    unimplemented!()
}
