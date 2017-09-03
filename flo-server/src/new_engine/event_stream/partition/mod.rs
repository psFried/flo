mod segment;
mod index;

use std::path::{Path, PathBuf};
use std::collections::VecDeque;

use new_engine::api::{Operation, NamespaceGlob};
use event::{ActorId, EventCounter};
use self::segment::Segment;


fn get_events_file(partition_dir: &Path, segment_num: SegmentNum) -> PathBuf {
    let filename = format!("{}.events", segment_num.0);
    partition_dir.join(filename)
}


/// A 1-based monotonically incrementing counter used to identify segments.
/// SegmentNums CANNOT BE 0! Since these are used all over in rather memory-sensitive areas, we sometimes
/// use 0 as a sentinal value to indicate the lack of a segment;
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct SegmentNum(u64);

impl SegmentNum {
    /// returns true if this segment is non-zero
    pub fn is_set(&self) -> bool {
        self.0 > 0
    }
}

pub struct PartitionImpl {
    event_stream_name: String,
    partition_num: ActorId,
    partition_dir: PathBuf,
    segments: VecDeque<Segment>,
}


