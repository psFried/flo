mod segment;

use std::path::{Path, PathBuf};
use std::collections::VecDeque;

use event::{ActorId, EventCounter};
use self::segment::Segment;


fn get_events_file(partition_dir: &Path, segment_num: u64) -> PathBuf {
    let filename = format!("{}.events", segment_num);
    partition_dir.join(filename)
}


pub struct PartitionImpl {
    event_stream_name: String,
    partition_num: ActorId,
    segments: VecDeque<Segment>,
}
