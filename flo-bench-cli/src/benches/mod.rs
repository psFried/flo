mod producer;

pub use self::producer::ProducerBenchmark;

use flo_client_lib::sync::SyncConnection;
use flo_client_lib::codec::RawCodec;


fn connect(addr: String) -> Result<SyncConnection<Vec<u8>>, String> {
    println!("Creating connection to: {}", addr);
    SyncConnection::connect_from_str(&addr, "flo-bench-cli", RawCodec, None).map_err(|io_err| {
        format!("{:?}", io_err)
    })
}
