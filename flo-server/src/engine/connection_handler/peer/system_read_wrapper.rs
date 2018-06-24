use event::{EventCounter, FloEvent};
use protocol::{self, ProtocolMessage, Term, FloInstanceId};
use engine::controller::{SystemEvent, SystemStreamReader, SYSTEM_READER_BATCH_SIZE};
use engine::event_stream::partition::PersistentEvent;
use engine::connection_handler::{CallAppendEntries, ConnectionHandlerResult};
use engine::connection_handler::connection_state::ConnectionState;


#[derive(Debug)]
pub struct SystemReaderWrapper {
    prev_event_index: EventCounter,
    prev_event_term: Term,
    last_sent_index: EventCounter,
    last_sent_term: Term,
    reader: SystemStreamReader,
    event_buffer: Vec<SystemEvent<PersistentEvent>>,
}

impl SystemReaderWrapper {
    pub fn new(common: &mut ConnectionState) -> SystemReaderWrapper {
        let connection_id = common.connection_id;
        let reader = common.get_system_stream().create_system_stream_reader(connection_id);
        SystemReaderWrapper {
            prev_event_index: 0,
            prev_event_term: 0,
            last_sent_index: 0,
            last_sent_term: 0,
            event_buffer: Vec::with_capacity(SYSTEM_READER_BATCH_SIZE),
            reader
        }
    }

    pub fn append_acknowledged(&mut self) -> EventCounter {
        self.prev_event_term = self.last_sent_term;
        self.prev_event_index = self.last_sent_index;
        self.last_sent_index
    }

    pub fn send_append_entries(&mut self, op_id: u32, this_instance_id: FloInstanceId, call: CallAppendEntries, common: &mut ConnectionState) -> ConnectionHandlerResult {

        let connection_id = common.connection_id;
        let CallAppendEntries {current_term, commit_index, reader_start_position} = call;

        if let Some(start) = reader_start_position {
            info!("Setting start position for outgoing AppendEntries for connection_id: {} to {:?}", connection_id, start);
            self.prev_event_term = start.prev_entry_term;
            self.prev_event_index = start.prev_entry_index;
            self.reader.set_to(start.reader_start_segment, start.reader_start_offset).map_err(|io_err| {
                format!("Failed to set reader position for connection_id: {} to: {:?}, io_err: {:?}", connection_id, start, io_err)
            })?; // early return on failure
        }

        let prev_term = self.prev_event_term;
        let prev_index = self.prev_event_index;

        let event_count = {
            let SystemReaderWrapper {ref mut event_buffer, ref mut reader, ..} = *self;
            event_buffer.clear();
            reader.fill_buffer(event_buffer).map_err(|io_err| {
                format!("Error reading system events for connection_id: {}, io_err: {:?}", connection_id, io_err)
            })? // early return on failure
        };

        let append = protocol::AppendEntriesCall {
            op_id,
            leader_id: this_instance_id,
            term: current_term,
            prev_entry_term: prev_term,
            prev_entry_index: prev_index,
            leader_commit_index: commit_index,
            entry_count: event_count as u32,
        };

        if let Some(event) = self.event_buffer.last() {
            self.last_sent_term = event.term();
            self.last_sent_index = event.id().event_counter;
        }

        common.send_to_client(ProtocolMessage::SystemAppendCall(append))?;
        for event in self.event_buffer.drain(..) {
            common.send_to_client(ProtocolMessage::ReceiveEvent(event.into()))?;
        }
        Ok(())
    }
}
