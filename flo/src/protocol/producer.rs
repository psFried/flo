
pub const TYPE_PRODUCE_EVENT: u8 = 1u8;
pub const TYPE_MODIFY_NAMESPACE: u8 = 4u8;

#[derive(Debug, PartialEq)]
pub enum ProducerMessage<'a> {
    ProduceEvent(ProduceEvent<'a>),
    ModifyNamespace(ModifyNamespace<'a>)
}

#[derive(Debug, PartialEq)]
pub struct ProduceEvent<'a> {
    pub tags: &'a str,
    pub event_data: &'a [u8],
}

#[derive(Debug, PartialEq)]
pub struct ModifyNamespace<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

