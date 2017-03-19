use std::io;
use std::net::ToSocketAddrs;

use sync::connection::SyncConnection;
use codec::{StringCodec, RawCodec};

// Re-export useful types here to make it easier for basic consumers
pub use event::{FloEventId, Timestamp};
pub use sync::connection::{Connection};

/// Establishes a new basic connection using a binary (no-op) codec to the given address
pub fn connect_with_binary_codec<T: ToSocketAddrs>(address: T) -> io::Result<Connection<Vec<u8>, RawCodec>> {
    SyncConnection::connect(address, RawCodec)
}

/// Establishes a new connection using a String codec allowing events to be consumed and produced using strings as the body
pub fn connect_with_string_codec<T: ToSocketAddrs>(address: T) -> io::Result<Connection<String, StringCodec>> {
    SyncConnection::connect(address, StringCodec)
}







