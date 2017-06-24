
use engine::api::ConsumerState;
use engine::consumer::filecursor::{Cursor, CursorImpl};
use engine::consumer::cache::Cache;
use engine::event_store::EventReader;
use protocol::ServerMessage;
use channels::Sender;


pub enum CursorType {
    File(Box<Cursor>),
    InMemory(ConsumerState)
}

pub trait ConnectionContext {
    fn start_consuming<S: Sender<ServerMessage> + 'static>(&mut self, consumer_state: ConsumerState, client_sender: &S) -> Result<CursorType, String>;
}


pub struct ConnectionContextImpl<'a, R: EventReader + 'a> {
    file_reader: &'a mut R,
    cache: &'a Cache,
}
