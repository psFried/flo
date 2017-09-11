
use std::sync::Arc;
use std::collections::HashMap;

use new_engine::EngineRef;
use new_engine::event_stream::{EventStreamRef, EventStreamOptions};

pub struct ControllerOptions {
    automatically_create_streams: bool,
    default_stream_options: EventStreamOptions,
}


/// A specialized event stream that always has exactly one partition and manages the cluster state and consensus
/// Of course there is no cluster state and thus no consensus at the moment, but we'll just leave this here...
pub struct FloController { // TODO: implement raft lol
    engine_ref: Arc<EngineRef>,
    event_streams: HashMap<String, EventStreamRef>,
}


pub fn start_controller() -> Arc<EngineRef> {
    unimplemented!()
}
