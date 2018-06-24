//! This is the wire protocol used to communicate between the server and client. Communication is done by sending and
//! receiving series' of distinct messages. Each message begins with an 8 byte header that identifies the type of message.
//! This is rather wasteful, but useful for the early stages when there's still a fair bit of debugging via manual inspection
//! of buffers. Messages are parsed using nom parser combinators, and serialized using simple a wrapper around a writer.
//!
//! The special cases in the protocol are for sending/receiving the events themselves. Since events can be quite large, they
//! are not actually implemented as a single message in the protocol, but rather as just a header. The header has all the basic
//! information as well as the length of the data portion (the body of the event). The event is read by first reading the
//! header and then reading however many bytes are indicated by the header for the body of the event.
//!
//! All numbers use big endian byte order.
//! All Strings are newline terminated.
pub mod flo_instance_id;
mod client_announce;
mod peer_announce;
mod error;
mod produce_event;
mod event_ack;
mod consume_start;
mod cursor_info;
mod event_stream_status;
mod set_event_stream;
mod receive_event;
mod append_entries;
mod request_vote;

use nom::{be_u64, be_u32, be_u16, be_u8};
use event::{time, OwnedFloEvent, FloEvent, FloEventId, Timestamp};
use serializer::Serializer;
use std::net::SocketAddr;

use self::client_announce::{parse_client_announce, serialize_client_announce};
use self::peer_announce::{parse_peer_announce, serialize_peer_announce};
use self::error::{parse_error_message, serialize_error_message};
use self::produce_event::{parse_new_producer_event, serialize_new_produce_header};
use self::event_ack::{parse_event_ack, serialize_event_ack};
use self::consume_start::{parse_new_start_consuming, serialize_consumer_start};
use self::cursor_info::{parse_cursor_created, serialize_cursor_created};
use self::event_stream_status::{parse_event_stream_status, serialize_event_stream_status};
use self::set_event_stream::{serialize_set_event_stream, parse_set_event_stream};
use self::receive_event::{serialize_receive_event_header, parse_receive_event_header};
use self::append_entries::{serialize_append_entries, parse_append_entries_call, serialize_append_response, parse_append_entries_response};
use self::request_vote::{serialize_request_vote_call, parse_request_vote_call, serialize_request_vote_response, parse_request_vote_response};

pub use self::client_announce::ClientAnnounce;
pub use self::peer_announce::{PeerAnnounce, ClusterMember};
pub use self::error::{ErrorMessage, ErrorKind};
pub use self::produce_event::ProduceEvent;
pub use self::event_ack::EventAck;
pub use self::consume_start::{NewConsumerStart, ConsumerFlags, CONSUME_UNLIMITED};
pub use self::cursor_info::CursorInfo;
pub use self::event_stream_status::{EventStreamStatus, PartitionStatus};
pub use self::set_event_stream::SetEventStream;
pub use self::append_entries::{AppendEntriesCall, AppendEntriesResponse};
pub use self::request_vote::{RequestVoteCall, RequestVoteResponse};
pub use self::flo_instance_id::FloInstanceId;

pub type Term = u64;

pub mod headers {
    pub const CLIENT_AUTH: u8 = 1;
    pub const UPDATE_MARKER: u8 = 4;
    pub const START_CONSUMING: u8 = 5;
    pub const AWAITING_EVENTS: u8 = 6;
    pub const PEER_UPDATE: u8 = 8;
    pub const CLUSTER_STATE: u8 = 11;
    pub const SET_BATCH_SIZE: u8 = 12;
    pub const NEXT_BATCH: u8 = 13;
    pub const END_OF_BATCH: u8 = 14;
    pub const STOP_CONSUMING: u8 = 15;
    pub const COMMIT_INDEX_UPDATED: u8 = 20;
}

use self::headers::*;

/// Defines all the distinct messages that can be sent over the wire between client and server.
#[derive(Debug, PartialEq, Clone)]
pub enum ProtocolMessage<E: FloEvent> {
    /// Sent from the server to client to acknowledge that an event was persisted successfully.
    AckEvent(EventAck),
    /// Sent by the server to an active consumer to indicate that it has reached the end of the stream. The server will
    /// continue to send events as more come in, but this just lets the client know that it may be some time before more
    /// events are available. This message will only be sent at most once to a given consumer.
    AwaitingEvents,
    /// Always the first message sent by the client to the server
    Announce(ClientAnnounce),
    /// Sent to consumers of uncommitted events to let them know when events become committed
    CommitIndexUpdated(FloEventId),
    /// send by the server to a client in response to a StartConsuming message to indicate the start of a series of events
    CursorCreated(CursorInfo),
    /// Sent by the server to notify a consumer that it has reached the end of a batch and that more events can be sent
    /// upon receipt of a `NextBatch` message by the server.
    EndOfBatch,
    /// Represents an error response to any other message
    Error(ErrorMessage),
    /// New message sent by a client to start reading events from the stream
    NewStartConsuming(NewConsumerStart),
    /// Sent by the client to tell the server that it is ready for the next batch
    NextBatch,
    /// Signals a client's intent to publish a new event. The server will respond with either an `EventAck` or an `ErrorMessage`
    ProduceEvent(ProduceEvent),
    /// Always the first message sent by a server to another server for a system stream connection
    PeerAnnounce(PeerAnnounce),
    /// This is a complete event as serialized over the wire. This message is sent to to both consumers as well as other servers
    ReceiveEvent(E),
    /// Set the event stream that the client will work with
    SetEventStream(SetEventStream),
    /// Sent in response to an Announce message. Contains basic information about the status of an event stream
    StreamStatus(EventStreamStatus),
    /// sent by a client to a server to tell the server to stop sending events. This is required in order to reuse the connection for multiple queries
    StopConsuming(u32),
    SystemAppendCall(AppendEntriesCall),
    SystemAppendResponse(AppendEntriesResponse),
    RequestVote(RequestVoteCall),
    VoteResponse(RequestVoteResponse),
}

named!{parse_str<String>,
    map_res!(
        length_data!(be_u16),
        |res| {
            ::std::str::from_utf8(res).map(|val| val.to_owned())
        }
    )
}



named!{parse_socket_addr<SocketAddr>, alt!(parse_socket_addr_v4 | parse_socket_addr_v6)}

named!{parse_socket_addr_v4<SocketAddr>,
    chain!(
        _tag: tag!(&[4u8]) ~
        one: be_u8 ~
        two: be_u8 ~
        three: be_u8 ~
        four: be_u8 ~
        port: be_u16,
        || {
            let ip = ::std::net::Ipv4Addr::new(one, two, three, four);
            let addr = ::std::net::SocketAddrV4::new(ip, port);
            SocketAddr::V4(addr)
        }
    )
}

named!{parse_socket_addr_v6<SocketAddr>,
    chain!(
        _tag: tag!(&[6u8]) ~
        a: be_u16 ~
        b: be_u16 ~
        c: be_u16 ~
        d: be_u16 ~
        e: be_u16 ~
        f: be_u16 ~
        g: be_u16 ~
        h: be_u16 ~
        port: be_u16,
        || {
            let ip = ::std::net::Ipv6Addr::new(a, b, c, d, e, f, g, h);
            ::std::net::SocketAddrV6::new(ip, port, 0, 0).into()
        }
    )
}

fn require_event_id(id: Option<FloEventId>) -> Result<FloEventId, &'static str> {
    id.ok_or("EventId must not be all zeros")
}

named!{parse_non_zero_event_id<FloEventId>,
    map_res!(parse_event_id, require_event_id)
}

named!{parse_zeroable_event_id<FloEventId>,
    chain!(
        counter: be_u64 ~
        actor: be_u16,
        || {
            FloEventId::new(actor, counter)
        }
    )
}

named!{parse_event_id<Option<FloEventId>>,
    chain!(
        counter: be_u64 ~
        actor: be_u16,
        || {
            if counter > 0 {
                Some(FloEventId::new(actor, counter))
            } else {
                None
            }
        }
    )
}

named!{parse_timestamp<Timestamp>,
    map!(be_u64, time::from_millis_since_epoch)
}


named!{parse_version_vec<Vec<FloEventId>>,
    length_count!(be_u16, parse_zeroable_event_id)
}

named!{parse_bool<bool>, map!(::nom::be_u8, |val| { val == 1 } )}

named!{parse_awaiting_events<ProtocolMessage<OwnedFloEvent>>, map!(tag!(&[AWAITING_EVENTS]), |_| {ProtocolMessage::AwaitingEvents})}

named!{parse_commit_index_updated<ProtocolMessage<OwnedFloEvent>>, chain!(
    _tag: tag!(&[COMMIT_INDEX_UPDATED]) ~
    id: parse_non_zero_event_id,
    || {
        ProtocolMessage::CommitIndexUpdated(id)
    }
)}

named!{parse_next_batch<ProtocolMessage<OwnedFloEvent>>, map!(tag!(&[NEXT_BATCH]), |_| {ProtocolMessage::NextBatch})}

named!{parse_end_of_batch<ProtocolMessage<OwnedFloEvent>>, map!(tag!(&[END_OF_BATCH]), |_| {ProtocolMessage::EndOfBatch})}

named!{parse_stop_consuming<ProtocolMessage<OwnedFloEvent>>, chain!(
    _tag: tag!(&[STOP_CONSUMING]) ~
    op_id: be_u32,
    || {
        ProtocolMessage::StopConsuming(op_id)
    }
)}


named!{pub parse_any<ProtocolMessage<OwnedFloEvent>>, alt!(
        parse_event_ack |
        parse_receive_event_header |
        parse_error_message |
        parse_awaiting_events |
        parse_new_producer_event |
        parse_next_batch |
        parse_commit_index_updated |
        parse_end_of_batch |
        parse_stop_consuming |
        parse_cursor_created |
        parse_new_start_consuming |
        parse_set_event_stream |
        parse_event_stream_status |
        parse_client_announce |
        parse_peer_announce |
        parse_append_entries_call |
        parse_append_entries_response |
        parse_request_vote_call |
        parse_request_vote_response
)}


impl <E: FloEvent> ProtocolMessage<E> {

    pub fn serialize(&self, buf: &mut [u8]) -> usize {
        match *self {
            ProtocolMessage::Announce(ref announce) => {
                serialize_client_announce(announce, buf)
            }
            ProtocolMessage::PeerAnnounce(ref announce) => {
                serialize_peer_announce(announce, buf)
            }
            ProtocolMessage::StreamStatus(ref status) => {
                serialize_event_stream_status(status, buf)
            }
            ProtocolMessage::SetEventStream(ref set_stream) => {
                serialize_set_event_stream(set_stream, buf)
            }
            ProtocolMessage::ReceiveEvent(ref event) => {
                serialize_receive_event_header(event, buf)
            }
            ProtocolMessage::CommitIndexUpdated(ref id) => {
                Serializer::new(buf)
                        .write_u8(COMMIT_INDEX_UPDATED)
                        .write_u64(id.event_counter)
                        .write_u16(id.actor)
                        .finish()
            }
            ProtocolMessage::CursorCreated(ref info) => {
                serialize_cursor_created(info, buf)
            }
            ProtocolMessage::AwaitingEvents => {
                Serializer::new(buf).write_u8(AWAITING_EVENTS).finish()
            }
            ProtocolMessage::StopConsuming(op_id) => {
                Serializer::new(buf)
                        .write_u8(headers::STOP_CONSUMING)
                        .write_u32(op_id)
                        .finish()
            }
            ProtocolMessage::ProduceEvent(ref header) => {
                serialize_new_produce_header(header, buf)
            }
            ProtocolMessage::NewStartConsuming(ref start) => {
                serialize_consumer_start(start, buf)
            }
            ProtocolMessage::AckEvent(ref ack) => {
                serialize_event_ack(ack, buf)
            }
            ProtocolMessage::Error(ref err_message) => {
                serialize_error_message(err_message, buf)
            }
            ProtocolMessage::NextBatch => {
                buf[0] = NEXT_BATCH;
                1
            }
            ProtocolMessage::EndOfBatch => {
                buf[0] = END_OF_BATCH;
                1
            }
            ProtocolMessage::SystemAppendCall(ref append) => {
                serialize_append_entries(append, buf)
            }
            ProtocolMessage::SystemAppendResponse(ref response) => {
                serialize_append_response(response, buf)
            }
            ProtocolMessage::RequestVote(ref request) => {
                serialize_request_vote_call(request, buf)
            }
            ProtocolMessage::VoteResponse(ref response) => {
                serialize_request_vote_response(response, buf)
            }
        }
    }

    pub fn get_body(&self) -> Option<&[u8]> {
        match *self {
            ProtocolMessage::ProduceEvent(ref produce) => {
                Some(produce.data.as_slice())
            }
            ProtocolMessage::ReceiveEvent(ref event) => {
                Some(event.data())
            }
            _ => None
        }
    }

    pub fn get_op_id(&self) -> u32 {
        match *self {
            ProtocolMessage::Announce(ref ann) => ann.op_id,
            ProtocolMessage::ProduceEvent(ref prod) => prod.op_id,
            ProtocolMessage::CursorCreated(ref info) => info.op_id,
            ProtocolMessage::Error(ref err) => err.op_id,
            ProtocolMessage::AckEvent(ref ack) => ack.op_id,
            ProtocolMessage::StreamStatus(ref status) => status.op_id,
            ProtocolMessage::SetEventStream(ref set) => set.op_id,
            ProtocolMessage::StopConsuming(ref op_id) => *op_id,
            _ => 0
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nom::{IResult, Needed};
    use event::{OwnedFloEvent, time, FloEventId};

    fn test_serialize_then_deserialize(message: &ProtocolMessage<OwnedFloEvent>) {
        let result  = ser_de(message);
        assert_eq!(*message, result);
    }

    fn ser_de(message: &ProtocolMessage<OwnedFloEvent>) -> ProtocolMessage<OwnedFloEvent> {
        serde_with_body(message, false)
    }

    fn serde_with_body(message: &ProtocolMessage<OwnedFloEvent>, include_body: bool) -> ProtocolMessage<OwnedFloEvent> {
        let mut buffer = [0; 1024];

        let mut len = message.serialize(&mut buffer[..]);
        if include_body {
            if let Some(body) = message.get_body() {
                (&mut buffer[len..(len + body.len())]).copy_from_slice(body);
                len += body.len();
            }
        }
        (&mut buffer[len..(len + 4)]).copy_from_slice(&[4, 3, 2, 1]); // extra bytes at the end of the buffer
        println!("buffer: {:?}", &buffer[..(len + 4)]);

        match parse_any(&buffer) {
            IResult::Done(remaining, result) => {
                assert!(remaining.starts_with(&[4, 3, 2, 1]));
                result
            }
            IResult::Error(err) => {
                panic!("Got parse error: {:?}", err)
            }
            IResult::Incomplete(need) => {
                panic!("Got incomplete: {:?}", need)
            }
        }

    }

    fn addr(addr_string: &str) -> SocketAddr {
        ::std::str::FromStr::from_str(addr_string).unwrap()
    }

    #[test]
    fn serde_commit_index_updated() {
        let message = ProtocolMessage::CommitIndexUpdated(FloEventId::new(5, 6));
        test_serialize_then_deserialize(&message);
    }

    #[test]
    fn serde_request_vote_response() {
        let response = RequestVoteResponse {
            op_id: 456,
            term: 577,
            vote_granted: true,
        };
        test_serialize_then_deserialize(&ProtocolMessage::VoteResponse(response));
    }

    #[test]
    fn serde_request_vote_call() {
        let request = RequestVoteCall {
            op_id: 3,
            term: 4,
            candidate_id: flo_instance_id::generate_new(),
            last_log_index: 567,
            last_log_term: 8910,
        };
        test_serialize_then_deserialize(&ProtocolMessage::RequestVote(request));
    }

    #[test]
    fn serde_append_entries_response() {
        let response = AppendEntriesResponse {
            op_id: 45,
            term: 345,
            success: false,
        };
        test_serialize_then_deserialize(&ProtocolMessage::SystemAppendResponse(response));
    }

    #[test]
    fn serde_append_entries_call() {
        let append = AppendEntriesCall {
            op_id: 345,
            leader_id: flo_instance_id::generate_new(),
            term: 987,
            prev_entry_term: 986,
            prev_entry_index: 134,
            leader_commit_index: 131,
            entry_count: 567,
        };

        test_serialize_then_deserialize(&ProtocolMessage::SystemAppendCall(append));
    }

    #[test]
    fn serde_client_announce() {
        let announce = ClientAnnounce {
            protocol_version: 1,
            op_id: 765,
            client_name: "nathan".to_owned(),
            consume_batch_size: Some(456),
        };
        test_serialize_then_deserialize(&ProtocolMessage::Announce(announce));
    }

    #[test]
    fn serde_peer_announce_with_ipv6_address() {
        let announce = PeerAnnounce {
            protocol_version: 9,
            op_id: 6543,
            instance_id: flo_instance_id::generate_new(),
            peer_address: addr("[1:3:5::2]:4321"),
            system_primary_id: Some(flo_instance_id::generate_new()),
            cluster_members: vec![
                ClusterMember {
                    id: flo_instance_id::generate_new(),
                    address: addr("1.2.3.4:5")
                },
                ClusterMember {
                    id: flo_instance_id::generate_new(),
                    address: addr("127.0.0.1:2456")
                },
                ClusterMember {
                    id: flo_instance_id::generate_new(),
                    address: addr("192.168.1.1:443")
                },
            ]
        };
        test_serialize_then_deserialize(&ProtocolMessage::PeerAnnounce(announce));
    }

    #[test]
    fn serde_peer_announce_with_ipv4_address() {
        let addr = ::std::str::FromStr::from_str("123.234.12.1:4321").unwrap();
        let announce = PeerAnnounce {
            protocol_version: 9,
            instance_id: flo_instance_id::generate_new(),
            peer_address: addr,
            op_id: 6543,
            system_primary_id: None,
            cluster_members: Vec::new(),
        };
        test_serialize_then_deserialize(&ProtocolMessage::PeerAnnounce(announce));
    }

    #[test]
    fn serde_event_stream_status() {
        let status = EventStreamStatus {
            op_id: 6425,
            name: "foo".to_owned(),
            partitions: vec![
                PartitionStatus {
                    partition_num: 1,
                    head: 638,
                    primary: true,
                    primary_server_address: Some(addr("173.255.32.4:876")),
                },
                PartitionStatus {
                    partition_num: 2,
                    head: 0,
                    primary: false,
                    primary_server_address: Some(addr("[56:78::1]:9001")),
                },
                PartitionStatus {
                    partition_num: 3,
                    head: 638,
                    primary: true,
                    primary_server_address: None,
                },
            ],
        };
        test_serialize_then_deserialize(&ProtocolMessage::StreamStatus(status));

        let status = EventStreamStatus {
            op_id: 0,
            name: "".to_owned(),
            partitions: Vec::new()
        };
        test_serialize_then_deserialize(&ProtocolMessage::StreamStatus(status));
    }

    #[test]
    fn serde_set_event_stream() {
        let set_stream = SetEventStream {
            op_id: 7264,
            name: "foo".to_owned()
        };
        test_serialize_then_deserialize(&ProtocolMessage::SetEventStream(set_stream));
    }

    #[test]
    fn serde_new_start_consuming() {
        let version_vec = vec![
            FloEventId::new(1, 5),
            FloEventId::new(3, 8),
            FloEventId::new(8, 5)
        ];
        test_serialize_then_deserialize(&ProtocolMessage::NewStartConsuming(NewConsumerStart{
            op_id: 321,
            options: ConsumerFlags::default(),
            version_vector: version_vec,
            max_events: 987,
            namespace: "/foo/bar/*".to_owned(),
        }));
    }

    #[test]
    fn serde_new_start_consuming_with_one_event() {
        let vv = vec![FloEventId::new(1, 0)];
        let msg = ProtocolMessage::NewStartConsuming(NewConsumerStart {
            op_id: 3,
            options: ConsumerFlags::CONSUME_UNCOMMITTED,
            version_vector: vv,
            max_events: 1,
            namespace: "/foo/*".to_owned(),
        });
        test_serialize_then_deserialize(&msg);
    }

    #[test]
    fn serde_receive_event() {
        let event = OwnedFloEvent::new(
            FloEventId::new(4, 5),
            Some(FloEventId::new(4, 3)),
            time::from_millis_since_epoch(99),
            "/foo/bar".to_owned(),
            vec![9; 99],
        );
        let message = ProtocolMessage::ReceiveEvent(event.clone());
        let result = serde_with_body(&message, true);
        assert_eq!(message, result);
    }

    #[test]
    fn stop_consuming_is_serialized_and_parsed() {
        test_serialize_then_deserialize(&ProtocolMessage::StopConsuming(345));
    }

    #[test]
    fn cursor_created_is_serialized_and_parsed() {
        test_serialize_then_deserialize(&ProtocolMessage::CursorCreated(CursorInfo{op_id: 543, batch_size: 78910}));
    }

    #[test]
    fn next_batch_is_serialized_and_parsed() {
        test_serialize_then_deserialize(&ProtocolMessage::NextBatch);
    }

    #[test]
    fn end_of_batch_is_serialized_and_parsed() {
        test_serialize_then_deserialize(&ProtocolMessage::EndOfBatch);
    }

    #[test]
    fn awaiting_events_message_is_serialized_and_parsed() {
        test_serialize_then_deserialize(&mut ProtocolMessage::AwaitingEvents);
    }

    #[test]
    fn error_message_is_parsed() {
        let error = ErrorMessage {
            op_id: 12345,
            kind: ErrorKind::InvalidNamespaceGlob,
            description: "some shit happened".to_owned(),
        };
        test_serialize_then_deserialize(&mut ProtocolMessage::Error(error));
    }

    #[test]
    fn acknowledge_event_message_is_parsed() {
        test_serialize_then_deserialize(&mut ProtocolMessage::AckEvent(EventAck{
            op_id: 2345667,
            event_id: FloEventId::new(123, 456),
        }));
    }

    #[test]
    fn parse_producer_event_parses_the_header_but_not_the_data() {
        let input = ProduceEvent::with_crc(
            9,
            7,
            "/the/namespace".to_owned(),
            Some(FloEventId::new(123, 456)),
            vec![9; 5]
        );
        let mut message_input = ProtocolMessage::ProduceEvent(input.clone());
        let message_result = ser_de(&mut message_input);

        if let ProtocolMessage::ProduceEvent(result) = message_result {
            assert_eq!(input.namespace, result.namespace);
            assert_eq!(input.parent_id, result.parent_id);
            assert_eq!(input.op_id, result.op_id);
            assert_eq!(input.partition, result.partition);

            // The vector must be allocated with the correct capacity, but we haven't actually read all the data
            assert_eq!(input.data.len(), result.data.capacity());
        } else {
            panic!("got the wrong fucking message. Just quit now");
        }
    }

    #[test]
    fn parse_string_returns_empty_string_string_length_is_0() {
        let input = vec![0, 0, 110, 4, 5, 6, 7];
        let (remaining, result) = parse_str(&input).unwrap();
        assert_eq!("".to_owned(), result);
        assert_eq!(&vec![110, 4, 5, 6, 7], &remaining);
    }

    #[test]
    fn string_is_serialized_and_parsed() {
        let input = "hello\n\tmoar bytes";
        let mut buffer = [0; 64];

        let n_bytes = Serializer::new(&mut buffer).write_string(input).finish();
        assert_eq!(19, n_bytes);

        let (_, result) = parse_str(&buffer[0..19]).unwrap();
        assert_eq!(input.to_owned(), result);
    }

    #[test]
    fn this_works_how_i_think_it_does() {
        let input = vec![
            3,
            0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 1, 34,  0, 1,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0,
            0, 0, 1, 93, 77, 45, 214, 26,
            47, 101, 118, 101
        ];

        let result = parse_any(&input);
        let expected = IResult::Incomplete(Needed::Size(12168));
        assert_eq!(expected, result);
    }
}
