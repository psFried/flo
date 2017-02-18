use std::net::SocketAddr;

use flo_sync_client::{SyncConnection, FloEventId};

pub struct ProduceOptions {
    pub server_address: SocketAddr,
    pub namespace: String,
    pub event_data: Vec<Vec<u8>>,
    pub parent_id: Option<FloEventId>,
}
