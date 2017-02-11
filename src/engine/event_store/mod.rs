mod index;
pub mod test_util;
pub mod fs;

use std::path::PathBuf;
use std::io;

use flo_event::{FloEvent, OwnedFloEvent, FloEventId};
use engine::version_vec::VersionVector;

pub struct StorageEngineOptions {
    pub storage_dir: PathBuf,
    pub root_namespace: String,
    pub max_events: usize,
}

pub trait EventReader: Sized {
    //TODO: bring sanity to error handling, maybe use std::error::Error, or else error_chain crate
    type Error: ::std::fmt::Debug;
    type Iter: Iterator<Item=Result<OwnedFloEvent, Self::Error>> + Send;

    fn load_range(&mut self, range_start: FloEventId, limit: usize) -> Self::Iter;

    fn get_highest_event_id(&mut self) -> FloEventId;
}

pub trait EventWriter: Sized {
    //TODO: bring sanity to error handling, maybe use std::error::Error, or else error_chain crate
    type Error: ::std::fmt::Debug;

    fn store<E: FloEvent>(&mut self, event: &E) -> Result<(), Self::Error>;
}

pub trait StorageEngine {
    type Writer: EventWriter;
    type Reader: EventReader;

    fn initialize(options: StorageEngineOptions) -> Result<(Self::Writer, Self::Reader, VersionVector), io::Error>;
}


