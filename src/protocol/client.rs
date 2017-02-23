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
use nom::{be_u64, be_u32, be_u16, IResult};
use event::{FloEventId, ActorId, Timestamp};
use serializer::Serializer;

use std::io::{self, Read};

pub mod headers {
    pub const CLIENT_AUTH: &'static str = "FLO_AUT\n";
    pub const PRODUCE_EVENT: &'static str = "FLO_PRO\n";
    pub const RECEIVE_EVENT: &'static str = "FLO_EVT\n";
    pub const UPDATE_MARKER: &'static str = "FLO_UMK\n";
    pub const START_CONSUMING: &'static str = "FLO_CNS\n";
    pub const AWAITING_EVENTS: &'static str = "FLO_AWT\n";
    pub const PEER_ANNOUNCE: &'static str = "FLO_PAN\n";
    pub const PEER_UPDATE: &'static str = "FLO_PUD\n";
    pub const EVENT_DELTA_HEADER: &'static str = "FLO_DEL\n";
    pub const ACK_HEADER: &'static [u8; 8] = b"FLO_ACK\n";
    pub const ERROR_HEADER: &'static [u8; 8] = b"FLO_ERR\n";
}

use self::headers::*;
pub const ERROR_INVALID_NAMESPACE: u8 = 15;
pub const ERROR_INVALID_CONSUMER_STATE: u8 = 16;

/// Describes the type of error. This gets serialized a u8
#[derive(Debug, PartialEq, Clone)]
pub enum ErrorKind {
    /// Indicates that the namespace provided by a consumer was an invalid glob pattern
    InvalidNamespaceGlob,
    /// Indicates that the client connection was in an invalid state when it attempted some consumer operation
    InvalidConsumerState,
}
unsafe impl Send for ErrorKind {}

/// Represents a response to any request that results in an error
#[derive(Debug, PartialEq, Clone)]
pub struct ErrorMessage {
    /// The op_id of the request to make it easier to correlate request/response pairs
    pub op_id: u32,

    /// The type of error
    pub kind: ErrorKind,

    /// A human-readable description of the error
    pub description: String,
}

impl ErrorKind {
    /// Converts from the serialized u8 to an ErrorKind
    pub fn from_u8(byte: u8) -> Result<ErrorKind, u8> {
        match byte {
            ERROR_INVALID_NAMESPACE => Ok(ErrorKind::InvalidNamespaceGlob),
            other => Err(other)
        }
    }

    /// Converts the ErrorKind to it's serialized u8 value
    pub fn u8_value(&self) -> u8 {
        match self {
            &ErrorKind::InvalidNamespaceGlob => ERROR_INVALID_NAMESPACE,
            &ErrorKind::InvalidConsumerState => ERROR_INVALID_CONSUMER_STATE
        }
    }
}

/// The header of an event that a client (producer) wishes to add to the stream. This message MUST always be directly
/// followed by the entire body of the event.
#[derive(Debug, PartialEq, Clone)]
pub struct ProduceEventHeader {
    /// An opaque, client-generated number that can be used to correlate request/response pairs. The response to producing
    /// an event will always be either an EventAck or an ErrorMessage with the same `op_id` as was sent in the header
    pub op_id: u32,

    /// The namespace to produce the event onto. Technically, a namespace can be any valid utf-8 string (except it cannot
    /// contain any newline `\n` characters), but by convention they take the form of a path with segments separated by
    /// forward slash `/` characters.
    pub namespace: String,

    /// Technically, this can be any FloEventId, but the convention is to set it to the id of the event that was being processed
    /// when this event was produced. This allows correlation of request and response, as well as more sophisticated things
    /// like tracing events through a complex system of services.
    pub parent_id: Option<FloEventId>,

    /// The length of the event data (body). This is the number of bytes that will be read immediately following the header.
    /// This _can_ be 0, as events with no extra data (just a header) are totally valid.
    pub data_length: u32,
}

/// Sent by the server to the producer of an event to acknowledge that the event was successfully persisted to the stream.
#[derive(Debug, PartialEq, Clone)]
pub struct EventAck {
    /// This will be set to the `op_id` that was sent in the `ProduceEventHeader`
    pub op_id: u32,

    /// The id that was assigned to the event. This id is immutable and must be the same across all servers in a flo cluster.
    pub event_id: FloEventId,
}
unsafe impl Send for EventAck {}

/// Works the same as the `ProduceEventHeader`, except for receiving events from the server. This header must be directly
/// followed by the event body
#[derive(Debug, PartialEq, Clone)]
pub struct ReceiveEventHeader {
    /// The id (primary key) of the event
    pub id: FloEventId,

    /// The parent_id of the event. Used to correlate events
    pub parent_id: Option<FloEventId>,

    /// The namespace that the event was produced in
    pub namespace: String,

    /// The UTC timestamp associated with the event. Note that this is NOT monotonic. Timestamps are sent over the wire
    /// as a u64 representing the number of milliseconds since the unix epoch.
    pub timestamp: Timestamp,

    /// The length of the event data immediately following the header
    pub data_length: u32,
}

/// Sent by a client to the server to begin reading events from the stream.
#[derive(Debug, PartialEq, Clone)]
pub struct ConsumerStart {
    /// The maximum number of events to consume. Set to `u64::MAX` if you want unlimited.
    pub max_events: u64,

    /// The namespace to consume from. This can be any valid glob pattern, to allow reading from multiple namespaces.
    pub namespace: String,
}


/// Defines all the distinct messages that can be sent over the wire between client and server.
#[derive(Debug, PartialEq, Clone)]
pub enum ProtocolMessage {
    /// Sent from a client (producer) to the server as a request to produce an event. See `ProduceEventHeader` docs for more.
    ProduceEvent(ProduceEventHeader),
    /// Sent from the server to client to acknowledge that an event was persisted successfully.
    AckEvent(EventAck),
    /// Functions similarly to `ProduceEventHeader`, except for sending complete events from the server to the client.
    ReceiveEvent(ReceiveEventHeader),
    /// Sent by a client to set it's current position in the event stream
    UpdateMarker(FloEventId),
    /// sent by a client to start reading events from the stream
    StartConsuming(ConsumerStart),
    /// Sent by the server to an active consumer to indicate that it has reached the end of the stream. The server will
    /// continue to send events as more come in, but this just lets the client know that it may be some time before more
    /// events are available. This message will only be sent at most once to a given consumer.
    AwaitingEvents,
    /// Sent between flo servers to announce their presence. Essentially makes a claim that the given server represents
    /// the given `ActorId`
    PeerAnnounce(ActorId),
    /// Sent between flo servers to provide the version vector of the peer
    PeerUpdate{
        /// The ActorId of the peer that is claiming to have the versions listed
        actor_id: ActorId,
        /// For each actor that this peer has knowledge of, this will contain the highest `EventCounter` currently known.
        version_vec: Vec<FloEventId>,
    },
    /// This is just a bit of speculative engineering, honestly. Just don't even bother using it.
    ClientAuth {
        namespace: String,
        username: String,
        password: String,
    },
    /// Represents an error response to any other message
    Error(ErrorMessage),
}

named!{pub parse_str<String>,
    map_res!(
        take_until_and_consume!("\n"),
        |res| {
            ::std::str::from_utf8(res).map(|val| val.to_owned())
        }
    )
}

named!{pub parse_auth<ProtocolMessage>,
    chain!(
        _tag: tag!(CLIENT_AUTH) ~
        namespace: parse_str ~
        username: parse_str ~
        password: parse_str,
        || {
            ProtocolMessage::ClientAuth {
                namespace: namespace,
                username: username,
                password: password,
            }
        }
    )
}

fn require_event_id(id: Option<FloEventId>) -> Result<FloEventId, &'static str> {
    id.ok_or("EventId must not be all zeros")
}

named!{parse_non_zero_event_id<FloEventId>,
    map_res!(parse_event_id, require_event_id)
}

named!{pub parse_event_id<Option<FloEventId>>,
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

named!{pub parse_producer_event<ProtocolMessage>,
    chain!(
        _tag: tag!(PRODUCE_EVENT) ~
        namespace: parse_str ~
        parent_id: parse_event_id ~
        op_id: be_u32 ~
        data_len: be_u32,
        || {
            ProtocolMessage::ProduceEvent(ProduceEventHeader{
                namespace: namespace.to_owned(),
                parent_id: parent_id,
                op_id: op_id,
                data_length: data_len
            })
        }
    )
}

named!{parse_timestamp<Timestamp>,
    map!(be_u64, ::time::from_millis_since_epoch)
}

named!{parse_receive_event_header<ProtocolMessage>,
    chain!(
        _tag: tag!(RECEIVE_EVENT) ~
        id: parse_non_zero_event_id ~
        parent_id: parse_event_id ~
        timestamp: parse_timestamp ~
        namespace: parse_str ~
        data_len: be_u32,
        || {
           ProtocolMessage::ReceiveEvent(ReceiveEventHeader {
                id: id,
                parent_id: parent_id,
                namespace: namespace,
                timestamp: timestamp,
                data_length: data_len,
            })
        }
    )
}

named!{parse_event_ack<ProtocolMessage>,
    chain!(
        _tag: tag!(ACK_HEADER) ~
        op_id: be_u32 ~
        counter: be_u64 ~
        actor_id: be_u16,
        || {
            ProtocolMessage::AckEvent(EventAck {
                op_id: op_id,
                event_id: FloEventId::new(actor_id, counter)
            })
        }
    )
}

named!{parse_update_marker<ProtocolMessage>,
    chain!(
        _tag: tag!(UPDATE_MARKER) ~
        counter: be_u64 ~
        actor: be_u16,
        || {
            ProtocolMessage::UpdateMarker(
                FloEventId::new(actor, counter)
            )
        }
    )
}

named!{parse_start_consuming<ProtocolMessage>,
    chain!(
        _tag: tag!(START_CONSUMING) ~
        namespace: parse_str ~
        count: be_u64,
        || {
            ProtocolMessage::StartConsuming(ConsumerStart {
                namespace: namespace,
                max_events: count,
            })
        }
    )
}

named!{parse_peer_announce<ProtocolMessage>,
    chain!(
        _tag: tag!(PEER_ANNOUNCE) ~
        actor_id: be_u16,
        || {
            ProtocolMessage::PeerAnnounce(actor_id)
        }
    )
}

named!{parse_version_vec<Vec<FloEventId>>,
    length_count!(be_u16, parse_non_zero_event_id)
}

named!{parse_peer_update<ProtocolMessage>,
    chain!(
        _tag: tag!(PEER_UPDATE) ~
        actor_id: be_u16 ~
        versions: parse_version_vec,
        || {
            ProtocolMessage::PeerUpdate{
                actor_id: actor_id,
                version_vec: versions,
            }
        }
    )
}

named!{parse_error_message<ProtocolMessage>,
    chain!(
        _tag: tag!(ERROR_HEADER) ~
        op_id: be_u32 ~
        kind: map_res!(take!(1), |res: &[u8]| {
            ErrorKind::from_u8(res[0])
        }) ~
        description: parse_str,
        || {
            ProtocolMessage::Error(ErrorMessage {
                op_id: op_id,
                kind: kind,
                description: description,
            })
        }
    )
}

named!{parse_awaiting_events<ProtocolMessage>, map!(tag!(AWAITING_EVENTS), |_| {ProtocolMessage::AwaitingEvents})}

named!{pub parse_any<ProtocolMessage>, alt!(
        parse_producer_event |
        parse_event_ack |
        parse_receive_event_header |
        parse_peer_update |
        parse_peer_announce |
        parse_update_marker |
        parse_start_consuming |
        parse_auth |
        parse_error_message |
        parse_awaiting_events
)}


fn serialize_produce_header(header: &ProduceEventHeader, mut buf: &mut [u8]) -> usize {

    let (counter, actor) = header.parent_id.map(|id| {
        (id.event_counter, id.actor)
    }).unwrap_or((0, 0));

    Serializer::new(buf).write_bytes(PRODUCE_EVENT)
            .newline_term_string(&header.namespace)
            .write_u64(counter)
            .write_u16(actor)
            .write_u32(header.op_id)
            .write_u32(header.data_length)
            .finish()
}

fn serialize_receive_event_header(header: &ReceiveEventHeader, buf: &mut [u8]) -> usize {
    Serializer::new(buf).write_bytes(RECEIVE_EVENT)
            .write_u64(header.id.event_counter)
            .write_u16(header.id.actor)
            .write_u64(header.parent_id.map(|id| id.event_counter).unwrap_or(0))
            .write_u16(header.parent_id.map(|id| id.actor).unwrap_or(0))
            .write_u64(::time::millis_since_epoch(header.timestamp))
            .newline_term_string(&header.namespace)
            .write_u32(header.data_length)
            .finish()
}

fn serialize_event_ack(ack: &EventAck, buf: &mut [u8]) -> usize {
    Serializer::new(buf).write_bytes(ACK_HEADER)
            .write_u32(ack.op_id)
            .write_u64(ack.event_id.event_counter)
            .write_u16(ack.event_id.actor)
            .finish()
}

fn serialize_error_message(err: &ErrorMessage, buf: &mut [u8]) -> usize {
    Serializer::new(buf).write_bytes(ERROR_HEADER)
            .write_u32(err.op_id)
            .write_u8(err.kind.u8_value())
            .newline_term_string(&err.description)
            .finish()
}



impl ProtocolMessage {

    pub fn serialize(&self, buf: &mut [u8]) -> usize {
        match *self {
            ProtocolMessage::AwaitingEvents => {
                Serializer::new(buf).write_bytes(AWAITING_EVENTS).finish()
            }
            ProtocolMessage::ProduceEvent(ref header) => {
                serialize_produce_header(header, buf)
            }
            ProtocolMessage::ReceiveEvent(ref header) => {
                serialize_receive_event_header(header, buf)
            }
            ProtocolMessage::StartConsuming(ConsumerStart{ref namespace, ref max_events}) => {
                Serializer::new(buf).write_bytes(START_CONSUMING)
                                    .newline_term_string(namespace)
                                    .write_u64(*max_events)
                                    .finish()
            }
            ProtocolMessage::UpdateMarker(id) => {
                Serializer::new(buf).write_bytes(UPDATE_MARKER)
                                    .write_u64(id.event_counter)
                                    .write_u16(id.actor)
                                    .finish()
            }
            ProtocolMessage::ClientAuth {ref namespace, ref username, ref password} => {
                Serializer::new(buf).write_bytes(CLIENT_AUTH)
                                    .newline_term_string(namespace)
                                    .newline_term_string(username)
                                    .newline_term_string(password)
                                    .finish()
            }
            ProtocolMessage::PeerUpdate {ref actor_id, ref version_vec} => {
                let mut serializer = Serializer::new(buf).write_bytes(PEER_UPDATE)
                                                         .write_u16(*actor_id)
                                                         .write_u16(version_vec.len() as u16);

                for id in version_vec.iter() {
                    serializer = serializer.write_u64(id.event_counter).write_u16(id.actor);
                }
                serializer.finish()
            }
            ProtocolMessage::PeerAnnounce(actor_id) => {
                Serializer::new(buf).write_bytes(headers::PEER_ANNOUNCE.as_bytes()).write_u16(actor_id).finish()
            }
            ProtocolMessage::AckEvent(ref ack) => {
                serialize_event_ack(ack, buf)
            }
            ProtocolMessage::Error(ref err_message) => {
                serialize_error_message(err_message, buf)
            }
        }
    }
}

impl Read for ProtocolMessage {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let n_bytes = self.serialize(buf);
        Ok(n_bytes)
    }
}

pub trait ClientProtocol {
    fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage>;
}

pub struct ClientProtocolImpl;

impl ClientProtocol for ClientProtocolImpl {
    fn parse_any<'a>(&'a self, buffer: &'a [u8]) -> IResult<&'a [u8], ProtocolMessage> {
        parse_any(buffer)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Read;
    use nom::IResult;
    use event::FloEventId;

    fn test_serialize_then_deserialize(mut message: ProtocolMessage) {
        let mut buffer = [0; 128];

        let len = message.read(&mut buffer[..]).expect("failed to serialize message to buffer");
        (&mut buffer[len..(len + 4)]).copy_from_slice(&[4, 3, 2, 1]); // extra bytes at the end of the buffer
        println!("buffer: {:?}", &buffer[..(len + 4)]);

        match parse_any(&buffer) {
            IResult::Done(remaining, result) => {
                assert_eq!(message, result);
                assert!(remaining.starts_with(&[4, 3, 2, 1]));
            }
            IResult::Error(err) => {
                panic!("Got parse error: {:?}", err)
            }
            IResult::Incomplete(need) => {
                panic!("Got incomplete: {:?}", need)
            }
        }
    }

    #[test]
    fn awaiting_events_message_is_serialized_and_parsed() {
        test_serialize_then_deserialize(ProtocolMessage::AwaitingEvents);
    }

    #[test]
    fn error_message_is_parsed() {
        let error = ErrorMessage {
            op_id: 12345,
            kind: ErrorKind::InvalidNamespaceGlob,
            description: "some shit happened".to_owned(),
        };
        test_serialize_then_deserialize(ProtocolMessage::Error(error));
    }

    #[test]
    fn acknowledge_event_message_is_parsed() {
        test_serialize_then_deserialize(ProtocolMessage::AckEvent(EventAck{
            op_id: 2345667,
            event_id: FloEventId::new(123, 456),
        }));
    }

    #[test]
    fn peer_announce_is_parsed() {
        test_serialize_then_deserialize(ProtocolMessage::PeerAnnounce(1234));
    }

    #[test]
    fn peer_update_is_parsed() {
        let version_vec = vec![
            FloEventId::new(1, 5),
            FloEventId::new(2, 7),
            FloEventId::new(5, 1)
        ];
        test_serialize_then_deserialize(ProtocolMessage::PeerUpdate {
            actor_id: 12345,
            version_vec: version_vec,
        });
    }

    #[test]
    fn event_marker_update_is_parsed() {
        test_serialize_then_deserialize(ProtocolMessage::UpdateMarker(FloEventId::new(2, 255)));
    }

    #[test]
    fn start_consuming_message_is_parsed() {
        test_serialize_then_deserialize(ProtocolMessage::StartConsuming(ConsumerStart{
            namespace: "/test/ns".to_owned(),
            max_events: 8766
        }));
    }

    #[test]
    fn parse_producer_event_parses_correct_event() {
        let input = ProtocolMessage::ProduceEvent(ProduceEventHeader {
            namespace: "/the/namespace".to_owned(),
            parent_id: Some(FloEventId::new(123, 456)),
            op_id: 9,
            data_length: 5,
        });
        test_serialize_then_deserialize(input);
        let input = ProtocolMessage::ProduceEvent(ProduceEventHeader {
            namespace: "/another/namespace".to_owned(),
            parent_id: None,
            op_id: 8,
            data_length: 999,
        });
        test_serialize_then_deserialize(input);
    }


    #[test]
    fn parse_client_auth_returns_incomplete_result_when_password_is_missing() {
        let mut input = Vec::new();
        input.extend_from_slice(b"FLO_AUT\n");
        input.extend_from_slice(b"hello\n");
        input.extend_from_slice(b"world\n");

        let result = parse_auth(&input);
        match result {
            IResult::Incomplete(_) => { }
            e @ _ => panic!("Expected Incomplete, got: {:?}", e)
        }
    }

    #[test]
    fn parse_client_auth_parses_valid_header_with_no_remaining_bytes() {
        test_serialize_then_deserialize(ProtocolMessage::ClientAuth {
            namespace: "hello".to_owned(),
            username: "usr".to_owned(),
            password: "pass".to_owned(),
        });
    }

    #[test]
    fn parse_client_auth_returns_error_result_when_namespace_contains_invalid_utf_characters() {
        let mut input = Vec::new();
        input.extend_from_slice(b"FLO_AUT\n");
        input.extend_from_slice(&vec![0, 0xC0, 0, 0, 2, 10]);
        input.extend_from_slice(b"usr\n");
        input.extend_from_slice(b"pass\n");
        let result = parse_auth(&input);
        assert!(result.is_err());
    }


    #[test]
    fn parse_string_returns_empty_string_when_first_byte_is_a_newline() {
        let input = vec![10, 4, 5, 6, 7];
        let (remaining, result) = parse_str(&input).unwrap();
        assert_eq!("".to_owned(), result);
        assert_eq!(&vec![4, 5, 6, 7], &remaining);
    }

    #[test]
    fn parse_string_returns_string_with_given_length() {
        let input = b"hello\nextra bytes";
        let (remaining, result) = parse_str(&input[..]).unwrap();
        assert_eq!("hello".to_owned(), result);
        let extra_bytes = b"extra bytes";
        assert_eq!(&extra_bytes[..], remaining);
    }
}
