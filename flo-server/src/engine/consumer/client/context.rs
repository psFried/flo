
use engine::api::ConsumerState;
use engine::consumer::filecursor::{Cursor, CursorImpl};
use engine::consumer::cache::Cache;
use engine::event_store::EventReader;
use protocol::ServerMessage;
use futures::sync::mpsc::UnboundedSender;


pub enum CursorType {
    File(Box<Cursor>),
    InMemory(ConsumerState)
}

pub trait ConnectionContext {
    fn start_consuming(&mut self, consumer_state: ConsumerState, client_sender: UnboundedSender<ServerMessage>) -> Result<CursorType, String>;
}


pub struct ConnectionContextImpl<'a, R: EventReader + 'a> {
    file_reader: &'a mut R,
    cache: &'a Cache,
}

impl <'a, R: EventReader + 'a> ConnectionContext for ConnectionContextImpl<'a, R> {
    fn start_consuming(&mut self, consumer_state: ConsumerState, client_sender: UnboundedSender<ServerMessage>) -> Result<CursorType, String> {
        unimplemented!()
    }
}