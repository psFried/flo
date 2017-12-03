
use engine::ReceivedProtocolMessage;

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

#[derive(Debug)]
pub enum ConnectionControl {
    InitiateOutgoingSystemConnection,
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
