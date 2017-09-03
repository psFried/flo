mod partition;
//mod consumer_manager;

use std::path::PathBuf;
use std::io;


use new_engine::api::{EventStreamOptions,
                      EventStreamRef,
                      PartitionRef,
                      EngineReceiver,
                      EngineSender,
                      ClientSender,
                      ClientMessageSender,
                      Operation};





pub fn init_event_stream(storage_dir: PathBuf, options: EventStreamOptions) -> Result<EventStreamRef, io::Error> {
    unimplemented!()
}





