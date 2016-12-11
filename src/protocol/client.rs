use nom::{be_u64, be_u32, be_u16, be_i64, IResult};
use flo_event::FloEventId;

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
        op_id: be_u32 ~
        data_len: be_u32,
        || {
            ProtocolMessage::ProduceEvent(EventHeader{
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

    #[test]
    fn event_marker_update_is_parsed() {
        let mut input = Vec::new();
        input.extend_from_slice(b"FLO_UMK\n");
        input.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 255]); //event counter
        input.extend_from_slice(&[0, 2]);  //actor id

        let expected = ProtocolMessage::UpdateMarker(FloEventId::new(2, 255));

        assert_parsed_eq(expected, parse_any(&input));
    }

    #[test]
    fn start_consuming_message_is_parsed() {
        let input = {
            let mut b = Vec::new();
            b.extend_from_slice(b"FLO_CNS\n");
            b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 196]);
            b
        };
        let expected = ProtocolMessage::StartConsuming(196);
        assert_parsed_eq(expected, parse_any(&input));
    }

    #[test]
    fn parse_producer_event_parses_correct_event() {
        let mut input = Vec::new();
        input.extend_from_slice(b"FLO_PRO\n");
        input.extend_from_slice(&[0, 0, 0, 9]); // op id
        input.extend_from_slice(&[0, 0, 0, 5]); // hacky way to set the length as a u32
        input.extend_from_slice(&[6, 7, 8]);

        let (remaining, result) = parse_producer_event(&input).unwrap();

        let expected = ProtocolMessage::ProduceEvent(EventHeader{op_id: 9, data_length: 5});
        assert_eq!(expected, result);
        assert_eq!(&[6, 7, 8], remaining);
    }


    #[test]
    fn parse_header_returns_incomplete_result_when_password_is_missing() {
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
    fn parse_header_parses_valid_header_with_no_remaining_bytes() {
        let mut input = Vec::new();
        input.extend_from_slice(b"FLO_AUT\n");
        input.extend_from_slice(b"hello\n");
        input.extend_from_slice(b"usr\n");
        input.extend_from_slice(b"pass\n");
        let (remaining, result) = parse_auth(&input).unwrap();

        let expected_header = ProtocolMessage::ClientAuth {
            namespace: "hello".to_owned(),
            username: "usr".to_owned(),
            password: "pass".to_owned(),
        };
        assert_eq!(expected_header, result);
        assert!(remaining.is_empty());
    }

    #[test]
    fn parse_header_returns_error_result_when_namespace_contains_invalid_utf_characters() {
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
