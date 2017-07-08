mod index;
pub mod fs;

#[cfg(test)]
#[allow(dead_code)]
pub mod test_util;

use std::path::PathBuf;
use std::io;

use chrono::Duration;
use event::{FloEvent, OwnedFloEvent, FloEventId, VersionVector};

#[derive(Clone, PartialEq)]
pub struct StorageEngineOptions {
    pub storage_dir: PathBuf,
    pub root_namespace: String,
    pub event_retention_duration: Duration,
    pub event_eviction_period: Duration,
}

pub trait EventReader: Sized {
    type Iter: Iterator<Item=Result<OwnedFloEvent, io::Error>> + Send;

    fn load_range(&mut self, range_start: &VersionVector, limit: usize) -> Result<Self::Iter, io::Error>;
}

pub trait EventWriter: Sized {
    fn store<E: FloEvent>(&mut self, event: &E) -> Result<(), io::Error>;
}

pub trait StorageEngine {
    type Writer: EventWriter;
    type Reader: EventReader;

    fn initialize(options: StorageEngineOptions) -> Result<(Self::Writer, Self::Reader, VersionVector), io::Error>;
}


