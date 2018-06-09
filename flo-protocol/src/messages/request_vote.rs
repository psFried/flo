use nom::{be_u32, be_u64};

use event::{EventCounter, OwnedFloEvent};
use super::{Term, FloInstanceId, ProtocolMessage, parse_bool};
use super::flo_instance_id::parse_flo_instance_id;
use serializer::Serializer;


pub const REQUEST_VOTE_CALL_HEADER: u8 = 32;
pub const REQUEST_VOTE_RESPONSE_HEADER: u8 = 33;

#[derive(Debug, Clone, PartialEq)]
pub struct RequestVoteCall {
    pub op_id: u32,
    pub term: Term,
    pub candidate_id: FloInstanceId,
    pub last_log_index: EventCounter,
    pub last_log_term: Term,
}


#[derive(Debug, Clone, PartialEq)]
pub struct RequestVoteResponse {
    pub op_id: u32,
    pub term: Term,
    pub vote_granted: bool,
}

pub fn serialize_request_vote_response(response: &RequestVoteResponse, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(REQUEST_VOTE_RESPONSE_HEADER)
            .write_u32(response.op_id)
            .write_u64(response.term)
            .write_bool(response.vote_granted)
            .finish()
}

named!{pub parse_request_vote_response<ProtocolMessage<OwnedFloEvent>>, do_parse!(
    tag!(&[REQUEST_VOTE_RESPONSE_HEADER])   >>
    op_id: be_u32                           >>
    term: be_u64                            >>
    vote_granted: parse_bool                >>

    ( ProtocolMessage::VoteResponse(RequestVoteResponse {
        op_id, term, vote_granted,
    }) )
)}

pub fn serialize_request_vote_call(req: &RequestVoteCall, buf: &mut [u8]) -> usize {
    Serializer::new(buf)
            .write_u8(REQUEST_VOTE_CALL_HEADER)
            .write_u32(req.op_id)
            .write_u64(req.term)
            .write_u64(req.candidate_id)
            .write_u64(req.last_log_index)
            .write_u64(req.last_log_term)
            .finish()
}

named!{pub parse_request_vote_call<ProtocolMessage<OwnedFloEvent>>, do_parse!(
    tag!(&[REQUEST_VOTE_CALL_HEADER])   >>
    op_id: be_u32                       >>
    term: be_u64                        >>
    candidate_id: parse_flo_instance_id >>
    last_log_index: be_u64              >>
    last_log_term: be_u64               >>

    ( ProtocolMessage::RequestVote(RequestVoteCall{
        op_id,
        term,
        candidate_id,
        last_log_index,
        last_log_term,
    }) )
)}
