
use engine::api::ConsumerState;
use engine::consumer::filecursor::Cursor;
use protocol::ServerMessage;
use channels::Sender;


pub enum CursorType {
    File(Box<Cursor>),
    InMemory(ConsumerState)
}

pub trait ConnectionContext {
    fn start_consuming<S: Sender<ServerMessage> + 'static>(&mut self, consumer_state: ConsumerState, client_sender: &S) -> Result<CursorType, String>;
}

