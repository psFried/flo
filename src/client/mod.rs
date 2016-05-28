mod producer;
mod consumer;


pub use self::producer::{FloProducer, ProducerResult, StreamProducerHandler, produce_stream, emit, emit_raw};
pub use self::consumer::{FloConsumer, run_consumer};

pub type StopResult = Result<(), String>;

#[derive(Debug, PartialEq)]
pub enum ConsumerCommand {
    Continue,
    Stop(StopResult)
}