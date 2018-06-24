
use protocol::Term;
use event::EventCounter;
use engine::ReceivedProtocolMessage;
use engine::controller::{CallRequestVote, VoteResponse, AppendEntriesResponse};
use engine::event_stream::partition::SegmentNum;

#[derive(Debug)]
pub enum ConnectionHandlerInput {
    IncomingMessage(ReceivedProtocolMessage),
    Control(ConnectionControl),
}

impl ConnectionHandlerInput {
    pub fn unwrap_protocol_message(self) -> ReceivedProtocolMessage {
        match self {
            ConnectionHandlerInput::IncomingMessage(message) => message,
            ConnectionHandlerInput::Control(ctrl) => {
                panic!("Attempt to unwrap a protocol message from a Control input with control: {:?}", ctrl);
            }
        }
    }
}



#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionControl {
    InitiateOutgoingSystemConnection,
    SendRequestVote(CallRequestVote),
    SendVoteResponse(VoteResponse),
    SendAppendEntries(CallAppendEntries),
    SendAppendEntriesResponse(AppendEntriesResponse),
}


impl From<ReceivedProtocolMessage> for ConnectionHandlerInput {
    fn from(message: ReceivedProtocolMessage) -> Self {
        ConnectionHandlerInput::IncomingMessage(message)
    }
}

impl From<ConnectionControl> for ConnectionHandlerInput {
    fn from(control: ConnectionControl) -> Self {
        ConnectionHandlerInput::Control(control)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallAppendEntries {
    pub current_term: Term,
    pub commit_index: EventCounter,
    pub reader_start_position: Option<AppendEntriesStart>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppendEntriesStart {
    pub prev_entry_index: EventCounter,
    pub prev_entry_term: Term,
    pub reader_start_offset: usize,
    pub reader_start_segment: SegmentNum,
}
