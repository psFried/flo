use nom::{be_u32, IResult};
use server::engine::api::{ClientMessage, ClientAuth};


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
        _tag: tag!("FLO_AUT\n") ~
        namespace: parse_str ~
        username: parse_str ~
        password: parse_str,
        || {
            ProtocolMessage::ApiMessage(ClientMessage::ClientAuth(ClientAuth {
                namespace: namespace,
                username: username,
                password: password,
            }))
        }
    )
}

named!{pub parse_producer_event<ProtocolMessage>,
    chain!(
        _tag: tag!("FLO_PRO\n") ~
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

named!{pub parse_any<ProtocolMessage>, alt!( parse_producer_event | parse_auth ) }

#[derive(Debug, PartialEq)]
pub struct EventHeader {
    pub op_id: u32,
    pub data_length: u32,
}

#[derive(Debug, PartialEq)]
pub enum ProtocolMessage {
    ProduceEvent(EventHeader),
    ApiMessage(ClientMessage),
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

        let expected_header = ProtocolMessage::ApiMessage(ClientMessage::ClientAuth(ClientAuth {
            namespace: "hello".to_owned(),
            username: "usr".to_owned(),
            password: "pass".to_owned(),
        }));
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
