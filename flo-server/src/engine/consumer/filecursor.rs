
use std::io;
use std::fmt::Debug;
use std::thread::{self, JoinHandle};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use futures::sync::mpsc as f_mpsc;

use event::{OwnedFloEvent, FloEventId, VersionVector};
use engine::api::{ConnectionId, ConsumerState, ConsumerManagerMessage};
use engine::event_store::EventReader;
use protocol::{ErrorMessage, ErrorKind, ProtocolMessage, ServerMessage};
use channels::Sender;

pub trait Cursor {
    fn send(&mut self, message: CursorMessage) -> Result<(), ()>;
    fn get_thread_name(&self) -> &str;

    fn continue_batch(&mut self) -> Result<(), ()> {
        self.send(CursorMessage::NextBatch)
    }

    fn close_cursor(&mut self) -> Result<(), ()> {
        self.send(CursorMessage::Close)
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum CursorMessage {
    Close,
    NextBatch
}

#[derive(Debug)]
pub struct CursorImpl {
    sender: std_mpsc::Sender<CursorMessage>,
    connection_id: ConnectionId,
    thread: JoinHandle<()>,
}

impl Cursor for CursorImpl {
    fn send(&mut self, message: CursorMessage) -> Result<(), ()> {
        self.sender.send(message).map_err(|_| ())
    }

    fn get_thread_name(&self) -> &str {
        // safe unwrap because we always set a thread name
        self.thread.thread().name().unwrap()
    }
}

impl PartialEq for Cursor {
    fn eq(&self, rhs: &Cursor) -> bool {
        self as *const Cursor == rhs as *const Cursor
    }
}

pub fn start<S: Sender<ServerMessage> + 'static, R: EventReader + 'static, C: Sender<ConsumerManagerMessage> + 'static>(mut state: ConsumerState,
                                                                           client_sender: S,
                                                                           mut reader: &mut R,
                                                                           consumer_manager_sender: C) -> io::Result<CursorImpl> {

    let connection_id = state.connection_id;
    let start = state.version_vector.min();
    let mut event_iter = reader.load_range(start, ::std::usize::MAX);

    let (engine_sender, my_receiver) = std_mpsc::channel::<CursorMessage>();
    let thread_name = format!("cursor for connection_id: {}", connection_id);

    let join_handle = thread::Builder::new().name(thread_name).spawn(move || {
        let mut cursor_closed = false;

        while !cursor_closed {
            while !cursor_closed && !state.is_batch_exhausted() {
                match send_next_event(&mut event_iter, &mut state, &client_sender) {
                    Ok(true) => {
                        // This means that an event was sent so we should just loop around
                    }
                    Ok(false) => {
                        // we've reached the end of stored events, so will transition to consuming from memory
                        consumer_manager_sender.send(ConsumerManagerMessage::FileCursorExhausted(state.clone())).unwrap();
                        cursor_closed = true;
                    }
                    Err(err) => {
                        cursor_closed = true;
                        let error_message = ErrorMessage {
                            op_id: 0,
                            kind: ErrorKind::StorageEngineError,
                            description: format!("{}", err)
                        };
                        client_sender.send(ServerMessage::Other(ProtocolMessage::Error(error_message))).unwrap();
                    }
                }

            }

            if state.is_batch_exhausted() && !cursor_closed {
                let message = ServerMessage::Other(ProtocolMessage::EndOfBatch);
                client_sender.send(message).unwrap();
                // wait for message to tell us when to proceed
                match my_receiver.recv() {
                    Ok(CursorMessage::NextBatch) => {
                        debug!("starting new batch for connection_id: {}", state.connection_id);
                        state.start_new_batch();
                    }
                    Ok(CursorMessage::Close) => {
                        debug!("closing cursor for connection_id: {}", state.connection_id);
                        cursor_closed = true;
                    }
                    Err(_) => {
                        cursor_closed = true;
                        warn!("ConsumerManager shutdown prior to cursor being closed. Closing cursor for connection_id: {}", state.connection_id);
                    }
                }
            }
        }

    });

    join_handle.map(|handle| {
        CursorImpl {
            sender: engine_sender,
            connection_id: connection_id,
            thread: handle,
        }
    })
}

fn send_next_event<I, S: Sender<ServerMessage>, E: Debug>(iter: &mut I, client: &mut ConsumerState, sender: &S) -> Result<bool, String> where I: Iterator<Item=Result<OwnedFloEvent, E>> {
    loop {
        let next_event = match iter.next() {
            Some(event_result) => {
                event_result.map_err(|err| {
                    format!("Failed to read event: {:?}", err)
                })? // early return if this is an error
            }
            None => {
                debug!("File cursor for connection_id: {} reached end of events file", client.connection_id);
                return Ok(false);
            }
        };

        if client.should_send_event(&next_event) {
            let event_id = next_event.id;
            trace!("Cursor for connection_id: {} sending event: {}", client.connection_id, next_event.id);
            let message = ServerMessage::Event(Arc::new(next_event));
            sender.send(message).map_err(|_| {
                format!("Error sending event to client io channel for connection_id: {}", client.connection_id)
            })?;
            client.event_sent(event_id);
            break
        }
    }
    Ok(true)
}
