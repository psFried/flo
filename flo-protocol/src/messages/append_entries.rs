
use std::net::SocketAddr;

use nom::{be_u64, be_u32, be_u8};
use serializer::Serializer;
use event::{EventCounter, OwnedFloEvent};
use super::{FloInstanceId, ProtocolMessage, Term};
use super::flo_instance_id::parse_flo_instance_id;


pub const APPEND_ENTRIES_CALL_HEADER: u8 = 30;
pub const APPEND_ENTRIES_RESPONSE_HEADER: u8 = 31;

#[derive(Debug, PartialEq, Clone)]
pub struct FloServer {
    pub id: FloInstanceId,
    pub address: SocketAddr,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AppendEntriesCall {
    pub op_id: u32,
    pub leader_id: FloInstanceId,
    pub term: Term,
    pub prev_entry_term: Term,
    pub prev_entry_index: EventCounter,
    pub leader_commit_index: EventCounter,
    pub entry_count: u32,
}

impl AppendEntriesCall {
    pub fn heartbeat(op_id: u32,
                     leader_id: FloInstanceId,
                     term: Term,
                     prev_entry_term: Term,
                     prev_entry_index: EventCounter,
                     leader_commit_index: EventCounter) -> AppendEntriesCall {

        AppendEntriesCall {
            op_id,
            leader_id,
            term,
            prev_entry_term,
            prev_entry_index,
            leader_commit_index,
            entry_count: 0,
        }
    }

    pub fn is_heartbeat(&self) -> bool {
        self.entry_count == 0
    }
}


#[derive(Debug, PartialEq, Clone)]
pub struct AppendEntriesResponse {
    pub op_id: u32,
    pub term: Term,
    pub success: bool,
}

pub fn serialize_append_response(response: &AppendEntriesResponse, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(APPEND_ENTRIES_RESPONSE_HEADER)
            .write_u32(response.op_id)
            .write_u64(response.term)
            .write_bool(response.success)
            .finish()
}

named!{pub parse_append_entries_response<ProtocolMessage<OwnedFloEvent>>, do_parse!(
    tag!(&[APPEND_ENTRIES_RESPONSE_HEADER]) >>
    op_id: be_u32                           >>
    term: be_u64                            >>
    success: map!(be_u8, |val| { val == 1 }) >>

    ( ProtocolMessage::SystemAppendResponse(AppendEntriesResponse {
        op_id, term, success,
    }) )
)}

pub fn serialize_append_entries(append: &AppendEntriesCall, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(APPEND_ENTRIES_CALL_HEADER)
            .write_u32(append.op_id)
            .write(&append.leader_id)
            .write_u64(append.term)
            .write_u64(append.prev_entry_term)
            .write_u64(append.prev_entry_index)
            .write_u64(append.leader_commit_index)
            .write_u32(append.entry_count)
            .finish()
}


named!{pub parse_append_entries_call<ProtocolMessage<OwnedFloEvent>>, do_parse!(
    tag!(&[APPEND_ENTRIES_CALL_HEADER]) >>
    op_id: be_u32    >>
    leader_id: parse_flo_instance_id    >>
    term: be_u64                        >>
    prev_entry_term: be_u64             >>
    prev_entry_index: be_u64            >>
    leader_commit_index: be_u64         >>
    entry_count: be_u32                 >>

    ( ProtocolMessage::SystemAppendCall(AppendEntriesCall {
        op_id,
        leader_id,
        term,
        prev_entry_term,
        prev_entry_index,
        leader_commit_index,
        entry_count
    }) )
)}
