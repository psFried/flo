
use event::EventCounter;
use engine::event_stream::partition::PartitionReader;
use super::{ConnectionState, ConnectionHandlerResult};

pub struct PeerFollowerState {
    last_acknowledged_id: EventCounter,
    reader: PartitionReader,
}



impl PeerFollowerState {
    pub fn new(last_acknowledged_id: EventCounter, reader: PartitionReader) -> PeerFollowerState {
        PeerFollowerState {
            last_acknowledged_id,
            reader
        }
    }


    pub fn send_append_entries(&mut self, connection_state: &mut ConnectionState) -> ConnectionHandlerResult {
        unimplemented!()
    }
}
