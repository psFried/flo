pub mod event_stream;
pub mod connection_handler;
pub mod controller;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize};
use std::net::SocketAddr;

use protocol::ProtocolMessage;
use event::{OwnedFloEvent, ActorId};
use self::event_stream::EventStreamRef;

pub use self::controller::{ControllerOptions, ClusterOptions, SystemStreamRef, start_controller};
pub use self::connection_handler::{ConnectionHandler, ConnectionHandlerResult};

pub type ConnectionId = usize;

use engine::event_stream::partition::PersistentEvent;

/// Thy type of messages that are received from clients
pub type ReceivedProtocolMessage = ProtocolMessage<OwnedFloEvent>;
/// The type of messages that are sent to client
pub type SendProtocolMessage = ProtocolMessage<PersistentEvent>;

pub type ClientSender = ::futures::sync::mpsc::UnboundedSender<SendProtocolMessage>;
pub type ClientReceiver = ::futures::sync::mpsc::UnboundedReceiver<SendProtocolMessage>;

pub fn create_client_channels() -> (ClientSender, ClientReceiver) {
    ::futures::sync::mpsc::unbounded()
}


pub static SYSTEM_STREAM_NAME: &'static str = "system";

pub fn system_stream_name() -> String {
    SYSTEM_STREAM_NAME.to_owned()
}

/// Returns the minimum number of votes required to achieve a majority. Takes as input the number of _other_ peers in the
/// cluster, not including this instance. Returns the number of votes required from _other_ peers, again not including the
/// implicit vote from this instance
fn minimum_required_votes_for_majority(number_of_other_peers: ActorId) -> ActorId {
    match number_of_other_peers {
        0 => 0,
        1 => 1,
        2 => 1,
        other @ _ if other % 2 == 0 => {
            other / 2
        }
        other @ _ => {
            (other / 2) + 1
        }
    }
}

#[derive(Clone, Debug)]
pub struct EngineRef {
    current_connection_id: Arc<AtomicUsize>,
    system_stream: SystemStreamRef,
    event_streams: Arc<Mutex<HashMap<String, EventStreamRef>>>
}

#[derive(Debug)]
pub enum ConnectError {
    InitFailed(::std::io::Error),
    NoStream,
}

impl EngineRef {
    pub fn new(system_stream: SystemStreamRef, event_streams: Arc<Mutex<HashMap<String, EventStreamRef>>>) -> EngineRef {
        EngineRef {
            current_connection_id: Arc::new(AtomicUsize::new(0)),
            system_stream,
            event_streams
        }
    }

    pub fn system_stream(&mut self) -> &mut SystemStreamRef {
        &mut self.system_stream
    }

    pub fn next_connection_id(&self) -> ConnectionId {
        let old = self.current_connection_id.fetch_add(1, ::std::sync::atomic::Ordering::SeqCst);
        old + 1
    }

    pub fn get_stream(&self, stream_name: &str) -> Result<EventStreamRef, ConnectError> {
        if stream_name == SYSTEM_STREAM_NAME {
            // user wants to interact with the system stream, so we'll convert it to a "normal" stream ref
            return Ok(self.system_stream.to_event_stream());
        }

        // go for a user stream
        let streams = self.event_streams.lock().unwrap();
        if let Some(stream) = streams.get(stream_name).map(|s| s.clone()) {
            Ok(stream)
        } else {
            Err(ConnectError::NoStream)
        }
    }

    pub fn get_default_stream(&self) -> EventStreamRef {
        let stream = {
            let guard = self.event_streams.lock().unwrap();
            guard.values().next().map(|stream| stream.clone())
        };
        stream.unwrap_or_else(|| {
            self.system_stream.to_event_stream()
        })
    }

    pub fn get_system_stream(&self) -> SystemStreamRef {
        self.system_stream.clone()
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn minimum_required_votes_for_majority_returns_0_when_there_are_no_other_peers() {
        // no-cluster mode, so no other votes are required
        let result = minimum_required_votes_for_majority(0);
        assert_eq!(0, result);
    }

    #[test]
    fn minimum_required_votes_for_majority_returns_1_when_there_is_1_other_peer() {
        // total cluster size is only 2, so they must both agree
        let result = minimum_required_votes_for_majority(1);
        assert_eq!(1, result);
    }

    #[test]
    fn minimum_required_votes_for_majority_returns_1_when_there_are_2_other_peers() {
        // total cluster size is 3, so one vote from another peer makes for 2/3
        let result = minimum_required_votes_for_majority(2);
        assert_eq!(1, result);
    }

    #[test]
    fn minimum_required_votes_for_majority_returns_2_when_there_are_3_other_peers() {
        // total cluster size is 4, so 2 votes makes for 3/4
        let result = minimum_required_votes_for_majority(3);
        assert_eq!(2, result);
    }

    #[test]
    fn minimum_required_votes_for_majority_returns_2_when_there_are_4_other_peers() {
        // total cluster size is 5, so 2 votes makes for 3/5
        let result = minimum_required_votes_for_majority(4);
        assert_eq!(2, result);
    }

    #[test]
    fn minimum_required_votes_for_majority_returns_4_when_there_are_7_other_peers() {
        // total cluster size is 8, so 4 votes makes for 5/8
        let result = minimum_required_votes_for_majority(7);
        assert_eq!(4, result);
    }
}


