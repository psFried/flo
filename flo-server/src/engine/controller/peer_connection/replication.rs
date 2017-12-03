use std::fmt::Debug;

use super::ConnectionSendResult;
use event::VersionVector;


/// trait representing an active peer connection to a particular event stream, with functions to control replication
pub trait PeerReplicationConnection: Debug + Send + 'static {
    fn start_replication(&mut self, version_vector: VersionVector) -> ConnectionSendResult<VersionVector>;
    fn stop_replication(&mut self) -> ConnectionSendResult<()>;
    fn close_connection(&mut self);
}



