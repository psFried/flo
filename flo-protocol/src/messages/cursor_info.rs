
use nom::be_u32;
use serializer::Serializer;
use event::OwnedFloEvent;
use super::ProtocolMessage;

pub const HEADER: u8 = 16;

/// Sent in a CursorCreated message from the server to a client to indicate that a cursor was successfully created.
/// Currently, this message only contains the batch size, but more fields may be added as they become necessary.
#[derive(Debug, PartialEq, Clone)]
pub struct CursorInfo {
    /// The operation id from the StartConsuming message that created this cursor.
    pub op_id: u32,

    /// The actual batch size that will be used by the server for sending events. Note that this value _may_ differ from the
    /// batch size that was explicitly set by the consumer, depending on server settings. This behavior is not currently
    /// implemented by the server, but it's definitely possible to change in the near future.
    pub batch_size: u32,
}

named!{pub parse_cursor_created<ProtocolMessage<OwnedFloEvent>>, chain!(
    _tag: tag!(&[HEADER]) ~
    op_id: be_u32 ~
    batch_size: be_u32,
    || {
        ProtocolMessage::CursorCreated(CursorInfo{
            op_id: op_id,
            batch_size: batch_size
        })
    }
)}

pub fn serialize_cursor_created(info: &CursorInfo, buf: &mut [u8]) -> usize {
    Serializer::new(buf).write_u8(HEADER)
                        .write_u32(info.op_id)
                        .write_u32(info.batch_size)
                        .finish()
}
