use event::{FloEventId, VersionVector};
use engine::api::{ConsumerState, ConnectionId, NamespaceGlob, ConsumerFilter};
use engine::consumer::client::context::CursorType;
use engine::consumer::filecursor::Cursor;
use protocol::{ErrorMessage, ErrorKind, ConsumerStart};

use std::fmt::{self, Debug};

#[derive(Debug, PartialEq, Clone)]
pub struct IdleState {
    pub version_vector: VersionVector,
    pub batch_size: u64,
}

impl IdleState {
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
    FileCursor(Box<Cursor>),
    InMemoryConsumer(ConsumerState),
}

impl PartialEq for ConnectionState {
    fn eq(&self, rhs: &Self) -> bool {
        use self::ConnectionState::*;

        match (self, rhs) {
            (&NotConsuming(ref l), &NotConsuming(ref r)) => l == r,
            (&FileCursor(ref l), &FileCursor(ref r)) => l.get_thread_name() == r.get_thread_name(),
            (&InMemoryConsumer(ref l), &InMemoryConsumer(ref r)) => l == r,
            _ => false
        }
    }
}

impl Debug for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ConnectionState::NotConsuming(ref idle) => {
                write!(f, "ConnectionState::NotConsuming({:?})", idle)
            }
            &ConnectionState::FileCursor(ref cursor) => {
                write!(f, "ConnectionState::FileCursor(thread = \"{}\")", cursor.get_thread_name())
            }
            &ConnectionState::InMemoryConsumer(ref state) => {
                write!(f, "ConnectionState::InMemoryConsumer({:?})", state)
            }
        }
    }
}

impl ConnectionState {
    pub fn init(batch_size: u64) -> ConnectionState {
        ConnectionState::NotConsuming(IdleState{
            version_vector: VersionVector::new(),
            batch_size: batch_size,
        })
    }

    pub fn new(cursor: CursorType) -> ConnectionState {
        match cursor {
            CursorType::File(file_cursor) => {
                ConnectionState::FileCursor(file_cursor)
            }
            CursorType::InMemory(consumer) => {
                ConnectionState::InMemoryConsumer(consumer)
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