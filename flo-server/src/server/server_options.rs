use std::path::PathBuf;
use chrono::Duration;
use std::net::SocketAddr;

use event::ActorId;


#[derive(Copy, Clone, PartialEq, Debug)]
#[allow(dead_code)]
pub enum MemoryUnit {
    Megabyte,
    Kilobyte,
    Byte
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct MemoryLimit {
    amount: usize,
    unit: MemoryUnit,
}


impl MemoryLimit {
    pub fn new(amount: usize, unit: MemoryUnit) -> MemoryLimit {
        MemoryLimit {
            amount: amount,
            unit: unit,
        }
    }

    pub fn as_bytes(&self) -> usize {
        let multiplier = match self.unit {
            MemoryUnit::Byte => 1,
            MemoryUnit::Kilobyte => 1024,
            MemoryUnit::Megabyte => 1024 * 1024,
        };
        multiplier * self.amount
    }
}

#[derive(PartialEq, Clone)]
pub struct ServerOptions {
    pub port: u16,
    pub data_dir: PathBuf,
    pub default_namespace: String,
    pub event_retention_duration: Duration,
    pub event_eviction_period: Duration,
    pub max_cached_events: usize,
    pub max_cache_memory: MemoryLimit,
    pub cluster_addresses: Option<Vec<SocketAddr>>,
    pub actor_id: ActorId,
    pub max_io_threads: Option<usize>,
}


impl ServerOptions {
    pub fn validate(&self) -> Result<(), String> {

        if self.event_eviction_period > self.event_retention_duration {
            return Err(format!("Event eviction period of {} hours cannot be greater than the retention duration of {} hours",
                               self.event_eviction_period.num_hours(),
                               self.event_retention_duration.num_hours()));
        }

        Ok(())
    }
}
