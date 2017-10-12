
use std::path::PathBuf;
use std::sync::Arc;
use std::collections::HashMap;
use std::io;

use new_engine::{EngineRef, SYSTEM_STREAM_NAME, system_stream_name};
use new_engine::event_stream::partition::{Operation, OpType, ProduceOperation, ConsumeOperation};
use new_engine::event_stream::{EventStreamRef,
                               EventStreamOptions,
                               init_existing_event_stream,
                               init_new_event_stream};

#[derive(Debug, PartialEq)]
pub struct ControllerOptions {
    pub storage_dir: PathBuf,
    pub default_stream_options: EventStreamOptions,
}


/// A specialized event stream that always has exactly one partition and manages the cluster state and consensus
/// Of course there is no cluster state and thus no consensus at the moment, but we'll just leave this here...
pub struct FloController { // TODO: implement raft lol
    engine_ref: Arc<EngineRef>,
    event_streams: HashMap<String, EventStreamRef>,
}


pub fn start_controller(options: ControllerOptions) -> io::Result<EngineRef> {
    use std::fs;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock, Mutex};
    use std::sync::atomic::AtomicUsize;
    use atomics::AtomicBoolWriter;

    debug!("Starting Flo Controller with: {:?}", options);

    let ControllerOptions{storage_dir, default_stream_options} = options;

    // for now, we'll just create a default "system" stream. This is temporary.
    // Once we start work on clustering, the system stream will be used exclusively for cluster communication
    // and other event stream(s) will be used for application data

    // There's only one machine, so all partitions will always be primary. Again, this is just temporary
    let status_writer = AtomicBoolWriter::with_value(true);

    let system_stream_dir = storage_dir.join(&default_stream_options.name);
    let event_stream_ref = if system_stream_dir.exists() {
        init_existing_event_stream(system_stream_dir, default_stream_options, status_writer.reader())?
    } else {
        init_new_event_stream(system_stream_dir, default_stream_options, status_writer.reader())?
    };

    let mut streams = HashMap::with_capacity(1);
    streams.insert(system_stream_name(), event_stream_ref);

    let engine = EngineRef {
        current_connection_id: Arc::new(AtomicUsize::new(0)),
        event_streams: Arc::new(Mutex::new(streams)),
    };
    Ok(engine)
}
