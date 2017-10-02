//! For running an event stream server in-process and using an in-memory transport for communication with it.
//! This is especially useful in development and testing, as it allows an application to run without a dependency
//! on an external server.

use new_engine::EngineRef;


pub struct EmbeddedFloServer {
    engine_ref: EngineRef,
}
