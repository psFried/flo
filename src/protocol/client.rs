use nom::{be_u64, be_u32, be_u16, be_i64, IResult};
use flo_event::FloEventId;
use std::io::{self, Read, Write};
use byteorder::{ByteOrder, BigEndian};

pub mod headers {
    pub const CLIENT_AUTH: &'static str = "FLO_AUT\n";
    pub const PRODUCE_EVENT: &'static str = "FLO_PRO\n";
    pub const UPDATE_MARKER: &'static str = "FLO_UMK\n";
    pub const START_CONSUMING: &'static str = "FLO_CNS\n";
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

named!{pub parse_producer_event<ProtocolMessage>,
    chain!(
        _tag: tag!(PRODUCE_EVENT) ~
        namespace: parse_str ~
        op_id: be_u32 ~
        data_len: be_u32,
        || {
            ProtocolMessage::ProduceEvent(EventHeader{
                namespace: namespace.to_owned(),
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
        count: be_i64,
        || {
            ProtocolMessage::StartConsuming(count)
        }
    )
}

named!{pub parse_any<ProtocolMessage>, alt!( parse_producer_event | parse_update_marker | parse_start_consuming | parse_auth ) }

#[derive(Debug, PartialEq)]
pub struct EventHeader {
    pub namespace: String,
    pub op_id: u32,
    pub data_length: u32,
}


#[derive(Debug, PartialEq)]
pub enum ProtocolMessage {
    ProduceEvent(EventHeader),
    UpdateMarker(FloEventId),
    StartConsuming(i64),
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

fn read_procude_header(header: &EventHeader, mut buf: &mut [u8]) -> Result<usize, io::Error> {
    set_header(buf, PRODUCE_EVENT);
    let mut pos = 8;
    pos += string_to_buffer(&mut buf[pos..], &header.namespace)?;

    BigEndian::write_u32(&mut buf[pos..(pos + 4)], header.op_id);
    pos += 4;

    BigEndian::write_u32(&mut buf[pos..(pos + 4)], header.data_length);
    Ok(pos + 4)
}

impl Read for ProtocolMessage {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let mut nread = 0;
        match *self {
            ProtocolMessage::ProduceEvent(ref header) => {
                read_procude_header(header, buf)
            }
            ProtocolMessage::StartConsuming(limit) => {
                set_header(buf, START_CONSUMING);
                BigEndian::write_i64(&mut buf[8..16], limit);
                Ok(16)
            }
            ProtocolMessage::UpdateMarker(id) => {
                set_header(buf, UPDATE_MARKER);
                BigEndian::write_u64(&mut buf[8..16], id.event_counter);
                BigEndian::write_u16(&mut buf[16..18], id.actor);
                Ok(18)
            }
            ProtocolMessage::ClientAuth {ref namespace, ref username, ref password} => {
                set_header(buf, CLIENT_AUTH);
                let mut pos = 8;
                pos += string_to_buffer(&mut buf[pos..], namespace)?;
                pos += string_to_buffer(&mut buf[pos..], username)?;
                pos += string_to_buffer(&mut buf[pos..], password)?;
                Ok(pos)
            }
        }
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
    use server::engine::api::{ClientMessage, ClientAuth};
    use flo_event::FloEventId;

    fn assert_parsed_eq(expected: ProtocolMessage, result: IResult<&[u8], ProtocolMessage>) {
        match result {
            IResult::Done(_rem, msg) => {
                assert_eq!(expected, msg);
            }
            IResult::Error(err) => panic!("Error parsing: {:?}", err),
            IResult::Incomplete(_) => panic!("Got incomplete result")
        }
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
    fn event_marker_update_is_parsed() {
        test_serialize_then_deserialize(ProtocolMessage::UpdateMarker(FloEventId::new(2, 255)));
    }

    #[test]
    fn start_consuming_message_is_parsed() {
        test_serialize_then_deserialize(ProtocolMessage::StartConsuming(214567));
    }

    #[test]
    fn parse_producer_event_parses_correct_event() {
        let input = ProtocolMessage::ProduceEvent(EventHeader {
            namespace: "/the/namespace".to_owned(),
            op_id: 9,
            data_length: 5,
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
