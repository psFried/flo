use std::collections::HashMap;
use std::io;

use event::EventCounter;
use protocol::{Term, FloInstanceId};
use engine::controller::cluster_state::peer_connections::PeerConnectionManager;
use engine::controller::ControllerState;
use engine::connection_handler::{ConnectionControl, AppendEntriesStart, CallAppendEntries};

#[derive(Debug)]
pub struct PrimaryState {
    term: Term,
    peer_positions: HashMap<FloInstanceId, Position>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum Position {
    SetStart(EventCounter),
    TrackingFrom(EventCounter),
}


impl PrimaryState {

    pub fn new(term: Term) -> PrimaryState {
        PrimaryState {
            term,
            peer_positions: HashMap::new()
        }
    }

    pub fn send_append_entries<I: Iterator<Item=FloInstanceId>>(&mut self, controller: &mut ControllerState,
                                                                connection_manager: &mut PeerConnectionManager,
                                                                all_peers: I) {

        let (commit_index, _commit_term) = match controller.get_last_committed() {
            Ok(result) => result,
            Err(io_err) => {
                error!("Unable to read last committed: {:?}", io_err);
                return;
            }
        };
        for peer in all_peers {
            let current_position = self.peer_positions.entry(peer).or_insert(Position::SetStart(commit_index));

            let (new_position, start) = match *current_position {
                Position::SetStart(new_start) => {
                    let append_start = match PrimaryState::get_start(new_start, controller) {
                        Ok(s) => s,
                        Err(io_err) => {
                            error!("Failed to get start position due to io error: {:?}, No more appendEntries will be sent", io_err);
                            return;
                        }
                    };
                    (new_start, Some(append_start))
                }
                Position::TrackingFrom(current) => {
                    (current, None)
                }
            };

            let append = CallAppendEntries {
                commit_index,
                current_term: self.term,
                reader_start_position: start,
            };
            connection_manager.send_to_peer(peer, ConnectionControl::SendAppendEntries(append));

            *current_position = Position::TrackingFrom(new_position);
        }
    }

    fn get_start(start_after_index: EventCounter, controller: &mut ControllerState) -> io::Result<AppendEntriesStart> {
        let (current_segment, current_offset) = controller.get_current_file_offset();

        if start_after_index == 0 {
            Ok(AppendEntriesStart {
                prev_entry_index: 0,
                prev_entry_term: 0,
                reader_start_offset: current_offset,
                reader_start_segment: current_segment,
            })
        } else {
            match controller.get_next_event(start_after_index.saturating_sub(1)) {
                Some(result) => {
                    let (prev_index, prev_term) = result?; // return error if there is one
                    let (start_segment, start_offset) = controller.get_next_entry(start_after_index).map(|entry| {
                        (entry.segment, entry.file_offset)
                    }).unwrap_or((current_segment, current_offset));

                    Ok(AppendEntriesStart {
                        prev_entry_index: prev_index,
                        prev_entry_term: prev_term,
                        reader_start_offset: start_offset,
                        reader_start_segment: start_segment,
                    })
                }
                None => {
                    // If we end up here, we've really fucked something up. If we have a non-zero number of events, then we
                    // should always have an event for the current position of each peer.
                    Err(io::Error::new(io::ErrorKind::InvalidInput, format!("Expected an event for index: {}, but no event was found", start_after_index)))
                }
            }

        }
    }

    pub fn append_entries_response(&mut self, peer_id: FloInstanceId, ack_through: Option<EventCounter>,
                                   controller: &mut ControllerState, connection_manager: &mut PeerConnectionManager) {
        if let Some(new_counter) = ack_through {
            // TODO: peer has acknowledged events through this index, tell the partitionImpl about the ack and see if it allows us to commit any events
            if log_enabled!(::log::Level::Debug) {
                let current = self.peer_positions.get(&peer_id);
                debug!("Received acknowledgement from peer_id: {:?}, new_position: {}, old_position: {:?}", peer_id, new_counter, current);
            }
            self.peer_positions.insert(peer_id, Position::TrackingFrom(new_counter));
        } else {
            // If the peer did not acknowledge the last append, then we take their last known position and move it back.
            // We move it back by 8, because that's the current batch size, but the actual amount is only a matter of efficiency.
            // This amount does not affect correctness.
            let current_position = self.get_peer_position(&peer_id);
            let new_position = current_position.saturating_sub(8);
            debug!("peer_id: {} did NOT acknowledge last append entries, changing position from {} to {}", peer_id, current_position, new_position);
            self.peer_positions.insert(peer_id, Position::SetStart(new_position));
        }
    }

    fn get_peer_position(&self, peer: &FloInstanceId) -> EventCounter {
        self.peer_positions.get(peer).map(|position| {
            match *position {
                Position::SetStart(start) => start,
                Position::TrackingFrom(last) => last
            }
        }).unwrap_or(0)
    }
}
