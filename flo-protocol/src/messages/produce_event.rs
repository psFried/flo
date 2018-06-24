
use nom::{be_u32, be_u16};
use event::{EventData, OwnedFloEvent, FloEventId, ActorId};
use serializer::Serializer;
use super::{ProtocolMessage, parse_str, parse_event_id};


pub const PRODUCE_EVENT: u8 = 2;


/// The body of a ProduceEvent `ProtocolMessage`. This is sent from a client producer to the server, and the server will
/// respond with either an `EventAck` or an `ErrorMessage` to indicate success or failure respectively. Although the flo
/// protocol is pipelined, this message includes an `op_id` field to aid in correlation of requests and responses.
#[derive(Debug, PartialEq, Clone)]
pub struct ProduceEvent {
    /// This is an arbritrary number, assigned by the client, to aid in correlation of requests and responses. Clients may
    /// choose to just set it to the same value for every operation if they wish.
    pub op_id: u32,
    /// The partition to produce the event onto
    pub partition: ActorId,
    /// The namespace to produce the event to. See the `namespace` documentation on `FloEvent` for more information on
    /// namespaces in general. As far as the protocol is concerned, it's just serialized as a utf-8 string.
    pub namespace: String,
    /// The parent_id of the new event. This is typically set to the id of whatever event a consumer is responding to.
    /// The parent id is optional. On the wire, a null parent_id is serialized as an event id where both the counter and the
    /// actor are set to 0.
    pub parent_id: Option<FloEventId>,
    /// The event payload. As far as the flo server is concerned, this is just an opaque byte array. Note that events with
    /// 0-length bodies are perfectly fine.
    pub data: Vec<u8>,
    /// The crc32c of the event
    pub crc: u32,
}

impl ProduceEvent {
    pub fn new(op_id: u32, partition: ActorId, namespace: String, parent_id: Option<FloEventId>, data: Vec<u8>, crc: u32) -> ProduceEvent {
        ProduceEvent {
            op_id, partition, namespace, parent_id, data, crc
        }
    }

    pub fn with_crc(op_id: u32, partition: ActorId, namespace: String, parent_id: Option<FloEventId>, data: Vec<u8>) -> ProduceEvent {
        let mut evt = ProduceEvent { op_id, partition, namespace, parent_id, data, crc: 0 };
        evt.set_crc();
        evt
    }

    pub fn set_crc(&mut self) {
        let new_crc = self.compute_data_crc();
        self.crc = new_crc;
    }
}

impl EventData for ProduceEvent {
    fn event_namespace(&self) -> &str {
        self.namespace.as_str()
    }
    fn event_parent_id(&self) -> Option<FloEventId> {
        self.parent_id
    }
    fn event_data(&self) -> &[u8] {
        self.data.as_slice()
    }
    fn get_precomputed_crc(&self) -> Option<u32> {
        Some(self.crc)
    }
}

named!{pub parse_new_producer_event<ProtocolMessage<OwnedFloEvent>>,
    chain!(
        _tag: tag!(&[PRODUCE_EVENT]) ~
        crc: be_u32 ~
        namespace: parse_str ~
        parent_id: parse_event_id ~
        op_id: be_u32 ~
        partition: be_u16 ~
        data_len: be_u32,
        || {
            ProtocolMessage::ProduceEvent(ProduceEvent{
                namespace: namespace.to_owned(),
                parent_id: parent_id,
                op_id: op_id,
                partition: partition,
                crc: crc,
                data: Vec::with_capacity(data_len as usize),
            })
        }
    )
}

pub fn serialize_new_produce_header(header: &ProduceEvent, buf: &mut [u8]) -> usize {
    let (counter, actor) = header.parent_id.map(|id| {
        (id.event_counter, id.actor)
    }).unwrap_or((0, 0));

    Serializer::new(buf).write_u8(PRODUCE_EVENT)
                        .write_u32(header.crc)
                        .write_string(&header.namespace)
                        .write_u64(counter)
                        .write_u16(actor)
                        .write_u32(header.op_id)
                        .write_u16(header.partition)
                        .write_u32(header.data.len() as u32)
                        .finish()
}
