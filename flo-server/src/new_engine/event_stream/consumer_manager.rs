


use std::collections::VecDeque;

use tokio_core::net::{TcpStream, Incoming};
use tokio_core::reactor::Remote;
use futures::{Future, Stream};
use memmap::MmapViewSync;

use event::ActorId;


struct EventStreamReader {
    partitions: VecDeque<PartitionReader>
}


struct PartitionReader {
    partition_num: ActorId,

    segments: Arc<Mutex<VecDeque<SegmentReader>>>,
}

struct IndexReader {
    index: Arc<RwLock<>>
}

struct SegmentReader {
    data: MmapViewSync,
}


fn setup_streams(tcp_stream: TcpStream, remote: Remote) {




}
