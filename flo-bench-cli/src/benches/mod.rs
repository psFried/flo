mod producer;

pub use self::producer::ProducerBenchmark;

use flo_client_lib::sync::connection::Connection;
use flo_client_lib::codec::RawCodec;


fn connect(addr: String) -> Result<Connection<RawCodec>, String> {
    println!("Creating connection to: {}", addr);
    Connection::connect(addr, RawCodec).map_err(|io_err| {
        format!("{:?}", io_err)
    })
}
