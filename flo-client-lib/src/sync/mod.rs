mod error;

use std::io;

use event::FloEventId;
use protocol::ProtocolMessage;
use ::Event;

pub mod connection;
pub mod basic;

pub use self::error::ClientError;

/// Trait used by a `SyncConnection` to handle communications between the client and server. A `Transport` handles
/// (de)serialization of `ProtocolMessage`s as well as delivery/receipt of the messages.
pub trait Transport: Sized {
    fn send(&mut self, message: ProtocolMessage) -> io::Result<()>;
    fn receive(&mut self) -> io::Result<ProtocolMessage>;
}

/// A Context is passed to a `Consumer` when it's `on_event` function is called. It allows the `Consumer` to publish
/// events in response to the current event that is being processed.
pub trait Context<Pro>: Sized {

    /// Returns the id of the event currently being processed
    fn current_event_id(&self) -> FloEventId;

    /// Returns the number of events remaining in the current batch
    fn batch_remaining(&self) -> u32;

    /// Produces an event to the given namespace with the `current_event_id` as it's parent. This is the primary method
    /// that consumers should use to produce events to the stream, as it automatically preserves the cause-effect relationship
    /// between events. This method produces the event synchronously and immediately. If this method returns a successful
    /// result, then the event is guaranteed to be durable on the node it was written to.
    fn respond<N: ToString, D: Into<Pro>>(&mut self, namespace: N, event_data: D) -> Result<FloEventId, ClientError>;

}

/// A consumer of events from the stream. `Consumer`s process events and may optionally respond to events using the `Context`.
pub trait Consumer<D> {
    fn name(&self) -> &str;

    /// Called when an event is successfully received. The `Context` allows responding to the event by producing
    /// additional events. Note if you produce to the same namespace that the consumer is listening on, then the consumer
    /// will receive it's own events. All responses are synchronous and immediate.
    fn on_event<C>(&mut self, event: Event<D>, context: &mut C) -> ConsumerAction where C: Context<D>;

    /// Called when there is some sort of error. The provided default action just logs the error using the `error!` macro
    /// from the log crate and then stops the consumer by calling `ConsumerAction::Stop`. Consumers can of course override
    /// this method to implement more sophisticated error handling such as reconnection strategies.
    fn on_error(&mut self, error: &ClientError) -> ConsumerAction {
        error!("Error running consumer: '{}' error: {:?}", self.name(), error);
        ConsumerAction::Stop
    }
}

/// `Consumer`s return a `ConsumerAction` on every invocation, which determines whether the consumer will continue consuming
/// or stop. `ConsumerAction` implements `From<Result<T, E>>`, so actions can be trivially derived from any `Result` by
/// calling `result.into()`. This converts a success into `Continue` and a failure into `Stop`.
pub enum ConsumerAction {
    /// signals that the consumer should be continued. If this is returned by `on_event` then it generally just means that
    /// the connection should continue on reading events as it already is. If this is returned by `on_error`, then it
    /// indicates that the consumer would like to recover from whatever the error is, which may entail re-establishing
    /// the connection if it has failed.
    Continue,

    /// signals that the consumer should be stopped and no further invocations of `on_event` or `on_error` should occur.
    /// The default `Consumer::on_error` method will always return `ConsumerAction::Stop`, meaning that it will never try
    /// to automatically recover from errors.
    Stop,
}

impl <T, E> From<Result<T, E>> for ConsumerAction {
    fn from(result: Result<T, E>) -> Self {
        if result.is_ok() {
            ConsumerAction::Continue
        } else {
            ConsumerAction::Stop
        }
    }
}

impl <F, D, R> Consumer<D> for F where F: Fn(Event<D>) -> R, R: Into<ConsumerAction> {
    fn name(&self) -> &str {
        "anonymous function consumer"
    }

    fn on_event<C>(&mut self, event: Event<D>, _: &mut C) -> ConsumerAction where C: Context<D> {
        self(event).into()
    }
}

