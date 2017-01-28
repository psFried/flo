use nom::{be_u64, be_u32, be_u16, be_i64, IResult};
use flo_event::{FloEventId, ActorId, EventCounter};
use byteorder::{ByteOrder, BigEndian};
use serializer::Serializer;

use std::io::{self, Read};
use std::collections::HashMap;

pub mod headers {
    pub const CLIENT_AUTH: &'static str = "FLO_AUT\n";
    pub const PRODUCE_EVENT: &'static str = "FLO_PRO\n";
    pub const UPDATE_MARKER: &'static str = "FLO_UMK\n";
    pub const START_CONSUMING: &'static str = "FLO_CNS\n";
    pub const PEER_ANNOUNCE: &'static str = "FLO_PAN\n";
    pub const PEER_UPDATE: &'static str = "FLO_PUD\n";
    pub const EVENT_DELTA_HEADER: &'static str = "FLO_DEL\n";
}

use self::headers::*;

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
            ProtocolMessage::ProduceEvent(EventHeader{
                namespace: namespace.to_owned(),
                parent_id: parent_id,
                op_id: op_id,
                data_length: data_len
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
        count: be_i64,
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

fn event_ids_to_map(ids: Vec<FloEventId>) -> HashMap<ActorId, EventCounter> {
    let mut map = HashMap::with_capacity(ids.len());
    for id in ids {
        map.insert(id.actor, id.event_counter);
    }
    map
}

named!{parse_version_map<HashMap<ActorId, EventCounter>>,
    map!(length_count!(be_u16, parse_non_zero_event_id), event_ids_to_map)
}

named!{parse_peer_update<ProtocolMessage>,
    chain!(
        _tag: tag!(PEER_UPDATE) ~
        actor_id: be_u16 ~
        versions: parse_version_map,
        || {
            ProtocolMessage::PeerUpdate{
                actor_id: actor_id,
                version_map: versions,
            }
        }
    )
}

named!{parse_event_delta_header<ProtocolMessage>,
    chain!(
        _tag: tag!(EVENT_DELTA_HEADER) ~
        actor_id: be_u16 ~
        versions: parse_version_map ~
        event_count: be_u32,
        || {
            ProtocolMessage::EventDeltaHeader{
                actor_id: actor_id,
                version_map: versions,
                event_count: event_count,
            }
        }
    )
}
named!{pub parse_any<ProtocolMessage>, alt!( parse_producer_event | parse_peer_update | parse_event_delta_header | parse_peer_announce | parse_update_marker | parse_start_consuming | parse_auth ) }

#[derive(Debug, PartialEq)]
pub struct EventHeader {
    pub op_id: u32,
    pub namespace: String,
    pub parent_id: Option<FloEventId>,
    pub data_length: u32,
}

#[derive(Debug, PartialEq)]
pub struct ConsumerStart {
    pub max_events: i64,
    pub namespace: String,
}


#[derive(Debug, PartialEq)]
pub enum ProtocolMessage {
    ProduceEvent(EventHeader),
    UpdateMarker(FloEventId),
    StartConsuming(ConsumerStart),
    PeerAnnounce(ActorId),
    PeerUpdate{
        actor_id: ActorId,
        version_map: HashMap<ActorId, EventCounter>
    },
    EventDeltaHeader{
        actor_id: ActorId,
        version_map: HashMap<ActorId, EventCounter>,
        event_count: u32,
    },
    ClientAuth {
        namespace: String,
        username: String,
        password: String,
    }
}


fn set_header(buf: &mut [u8], header: &'static str) {
    (&mut buf[..8]).copy_from_slice(header.as_bytes());
}

fn string_to_buffer(mut buf: &mut [u8], string: &String) -> Result<usize, io::Error> {
    let str_len = string.len();
    (&mut buf[..str_len]).copy_from_slice(string.as_bytes());
    buf[str_len] = b'\n';
    Ok(str_len + 1)
}

fn serialize_produce_header(header: &EventHeader, mut buf: &mut [u8]) -> usize {

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

impl Read for ProtocolMessage {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let n_bytes = match *self {
            ProtocolMessage::ProduceEvent(ref header) => {
                serialize_produce_header(header, buf)
            }
            ProtocolMessage::StartConsuming(ConsumerStart{ref namespace, ref max_events}) => {
                Serializer::new(buf).write_bytes(START_CONSUMING)
                        .newline_term_string(namespace)
                        .write_i64(*max_events)
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
            ProtocolMessage::PeerUpdate {ref actor_id, ref version_map} => {
                let mut serializer = Serializer::new(buf).write_bytes(PEER_UPDATE)
                        .write_u16(*actor_id)
                        .write_u16(version_map.len() as u16);

                for (actor, counter) in version_map.iter() {
                    serializer = serializer.write_u64(*counter).write_u16(*actor);
                }
                serializer.finish()
            }
            ProtocolMessage::PeerAnnounce(actor_id) => {
                Serializer::new(buf).write_bytes(headers::PEER_ANNOUNCE.as_bytes()).write_u16(actor_id).finish()
            }
            ProtocolMessage::EventDeltaHeader {ref actor_id, ref version_map, ref event_count} => {
                let mut serializer = Serializer::new(buf)
                        .write_bytes(headers::EVENT_DELTA_HEADER.as_bytes())
                        .write_u16(*actor_id)
                        .write_u16(version_map.len() as u16); //safe cast since we should never have more than 2^16 actors in the system

                for (actor_id, event_counter) in version_map.iter() {
                    serializer = serializer.write_u64(*event_counter).write_u16(*actor_id);
                }
                serializer.write_u32(*event_count).finish()
            }
        };
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
    use flo_event::FloEventId;
    use std::collections::HashMap;

    macro_rules! hashmap {
        ($($key:expr => $val:expr),*) => {{
            let mut m = HashMap::new();
            $(
                m.insert($key, $val);
            )*
            m
        }}
    }

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
    fn event_delta_header_is_parsed() {
        let versions = hashmap!(1 => 3, 3 => 88, 4 => 72);
        let header = ProtocolMessage::EventDeltaHeader {
            actor_id: 123,
            version_map: versions,
            event_count: 3,
        };
        test_serialize_then_deserialize(header);
    }

    #[test]
    fn peer_announce_is_parsed() {
        test_serialize_then_deserialize(ProtocolMessage::PeerAnnounce(1234));
    }

    #[test]
    fn peer_update_is_parsed() {
        let mut version_map = HashMap::new();
        version_map.insert(1, 5);
        version_map.insert(2, 7);
        version_map.insert(5, 1);
        test_serialize_then_deserialize(ProtocolMessage::PeerUpdate {
            actor_id: 12345,
            version_map: version_map,
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
        let input = ProtocolMessage::ProduceEvent(EventHeader {
            namespace: "/the/namespace".to_owned(),
            parent_id: Some(FloEventId::new(123, 456)),
            op_id: 9,
            data_length: 5,
        });
        test_serialize_then_deserialize(input);
        let input = ProtocolMessage::ProduceEvent(EventHeader {
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
