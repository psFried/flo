mod connection;

#[cfg(feature = "serde-client")]
mod serde;

pub use self::connection::{
    SyncConnection,
    ConsumerOptions,
    ConsumerAction,
    FloConsumer,
    ConsumerContext,
    ClientError,
    IoStream
};

use std::io;
use std::net::{TcpStream, ToSocketAddrs};

use protocol::{ProtocolMessage, ProduceEvent, ConsumerStart, ErrorMessage};
use event::{FloEventId, OwnedFloEvent};
use std::collections::VecDeque;







