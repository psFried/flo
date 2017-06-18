use event::{FloEventId, VersionVector, OwnedFloEvent};
use engine::api::{ConsumerState, ConnectionId, NamespaceGlob, ConsumerFilter};
use engine::consumer::client::context::CursorType;
use engine::consumer::filecursor::Cursor;
use protocol::{ErrorMessage, ErrorKind, ConsumerStart};

use std::sync::Arc;
use std::fmt::{self, Debug};

#[derive(Debug, PartialEq, Clone)]
pub struct IdleState {
    pub version_vector: VersionVector,
    pub batch_size: u64,
}

impl IdleState {
    fn new(batch_size: u64) -> IdleState {
        IdleState {
            version_vector: VersionVector::new(),
            batch_size: batch_size
        }
    }

    fn create_consumer_state(&self, connection_id: ConnectionId, start_message: &ConsumerStart) -> Result<ConsumerState, ErrorMessage> {
        NamespaceGlob::new(&start_message.namespace).map(|glob| {
            ConsumerState{
                version_vector: self.version_vector.clone(),
                filter: ConsumerFilter::Namespace(glob),
                connection_id: connection_id,
                batch_remaining: self.batch_size,
                batch_size: self.batch_size,
            }
        }).map_err(|err| {
            ErrorMessage {
                op_id: 0,
                kind: ErrorKind::InvalidNamespaceGlob,
                description: err,
            }
        })
    }
}


pub enum ConnectionState {
    NotConsuming(IdleState),
    FileCursor(Box<Cursor>, u64),
    InMemoryConsumer(ConsumerState),
}

impl PartialEq for ConnectionState {
    fn eq(&self, rhs: &Self) -> bool {
        use self::ConnectionState::*;

        match (self, rhs) {
            (&NotConsuming(ref l), &NotConsuming(ref r)) => l == r,
            (&FileCursor(ref l, ref l_batch), &FileCursor(ref r, ref r_batch)) => {
                l.get_thread_name() == r.get_thread_name() && l_batch == r_batch
            },
            (&InMemoryConsumer(ref l), &InMemoryConsumer(ref r)) => l == r,
            _ => false
        }
    }
}

fn log_warning<T, E: Debug>(result: Result<T, E>, message: &'static str) {
    if let Err(e) = result {
        warn!("{}, Err: {:?}", message, e)
    }
}

impl Debug for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ConnectionState::NotConsuming(ref idle) => {
                write!(f, "ConnectionState::NotConsuming({:?})", idle)
            }
            &ConnectionState::FileCursor(ref cursor, ref batch_size) => {
                write!(f, "ConnectionState::FileCursor(thread = \"{}\", batch_size = {})", cursor.get_thread_name(), batch_size)
            }
            &ConnectionState::InMemoryConsumer(ref state) => {
                write!(f, "ConnectionState::InMemoryConsumer({:?})", state)
            }
        }
    }
}

impl ConnectionState {
    pub fn init(batch_size: u64) -> ConnectionState {
        ConnectionState::NotConsuming(IdleState::new(batch_size))
    }

    pub fn new(cursor: CursorType, batch_size: u64) -> ConnectionState {
        match cursor {
            CursorType::File(file_cursor) => {
                ConnectionState::FileCursor(file_cursor, batch_size)
            }
            CursorType::InMemory(consumer) => {
                ConnectionState::InMemoryConsumer(consumer)
            }
        }
    }

    pub fn stop_consuming(&mut self) -> Result<(), ErrorMessage> {
        if let ConnectionState::FileCursor(ref mut cursor, _) = *self {
            // if there's a cursor open, then we need to close it down
            log_warning(cursor.close_cursor(), "Error closing cursor after client stopped consuming");
        }

        // Only reset the state if it is in a consuming state. This allows consumers to send multiple stop_consuming
        // messages without accidentally resetting their version vector
        if self.is_consuming() {
            let batch_size = self.get_batch_size();
            let new_state = IdleState::new(batch_size);
            *self = ConnectionState::NotConsuming(new_state);
        }
        // TODO: think about maybe propagating the error from closing the cursor. Not sure if that's appropriate or not
        Ok(())
    }

    pub fn get_batch_size(&self) -> u64 {
        match self {
            &ConnectionState::InMemoryConsumer(ref state) => state.batch_size,
            &ConnectionState::FileCursor(_, batch_size) => batch_size,
            &ConnectionState::NotConsuming(ref idle) => idle.batch_size
        }
    }

    pub fn is_consuming(&self) -> bool {
        match self {
            &ConnectionState::InMemoryConsumer(_) => true,
            &ConnectionState::FileCursor(_, _) => true,
            &ConnectionState::NotConsuming(_) => false
        }
    }

    pub fn should_send_event(&self, event: &OwnedFloEvent) -> bool {
        match self {
            &ConnectionState::InMemoryConsumer(ref state) => {
                state.should_send_event(event)
            }
            _ => false
        }
    }

    pub fn event_sent(&mut self, event_id: FloEventId) {
        match self {
            &mut ConnectionState::InMemoryConsumer(ref mut state) => {
                state.event_sent(event_id);
                return;
            }
            other @ _ => {
                error!("event_sent while in an invalid state: {:?}", other);
            }
        }
    }

    pub fn create_consumer_state(&self, connection_id: ConnectionId, start: &ConsumerStart) -> Result<ConsumerState, ErrorMessage> {
        match *self {
            ConnectionState::NotConsuming(ref idle_state) => {
                idle_state.create_consumer_state(connection_id, start)
            }
            _ => {
                Err(ErrorMessage {
                    op_id: 0,
                    kind: ErrorKind::InvalidConsumerState,
                    description: "Cannot start consuming while already in a consuming state".to_owned()
                })
            }
        }
    }

    pub fn update_version_vector(&mut self, id: FloEventId) -> Result<(), ErrorMessage> {
        self.require_idle_state("Cannot set position while already consuming", |idle| {
            idle.version_vector.set(id);
        })
    }

    pub fn set_batch_size(&mut self, batch_size: u64) -> Result<(), ErrorMessage> {
        self.require_idle_state("Cannot set batch size while consuming", |idle| idle.batch_size = batch_size)
    }

    fn require_idle_state<F>(&mut self, error_desc: &'static str, with_state: F) -> Result<(), ErrorMessage> where F: Fn(&mut IdleState) {
        match *self {
            ConnectionState::NotConsuming(ref mut idle) => {
                with_state(idle);
                Ok(())
            }
            _ => {
                Err(ErrorMessage {
                    op_id: 0,
                    kind: ErrorKind::InvalidConsumerState,
                    description: error_desc.to_owned()
                })
            }
        }

    }
}
