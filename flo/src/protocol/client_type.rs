pub const TYPE_CONSUMER: u8 = 2;
pub const TYPE_PRODUCER: u8 = 1;

#[derive(Debug, PartialEq)]
pub enum ClientType {
    Producer,
    Consumer
}

impl ClientType {
    pub fn from_byte(byte: u8) -> Result<ClientType, ()> {
        match byte {
            TYPE_PRODUCER => Ok(ClientType::Producer),
            TYPE_CONSUMER => Ok(ClientType::Consumer),
            _ => Err(())
        }
    }

}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn client_type_from_byte_returns_producer_for_01() {
        let result = ClientType::from_byte(1u8);
        assert_eq!(Ok(ClientType::Producer), result);
    }

    #[test]
    fn client_type_from_byte_returns_consumer_for_02() {
        let result = ClientType::from_byte(2u8);
        assert_eq!(Ok(ClientType::Consumer), result);
    }

    #[test]
    fn client_type_from_byte_returns_error_for_all_other_bytes_besides_01_or_02() {
        for byte in 3..255u8 {
            assert_eq!(Err(()), ClientType::from_byte(byte), "failed on byte: {}", byte);

        }
        assert_eq!(Err(()), ClientType::from_byte(0u8));
    }

}
