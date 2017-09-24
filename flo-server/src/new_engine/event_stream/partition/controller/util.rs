
use std::io;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Instant, Duration};

use new_engine::event_stream::partition::segment::{Segment, SegmentReader};
use new_engine::event_stream::partition::{SegmentNum, DATA_FILE_EXTENSION, SharedReaderRefsMut};
use new_engine::event_stream::partition::index::{PartitionIndex, IndexEntry};
use event::FloEvent;

#[derive(Debug)]
pub struct SegmentFile {
    pub segment_num: SegmentNum,
    pub path: PathBuf,
}

impl SegmentFile {
    pub fn init_segment(&self, partition_index: &mut PartitionIndex) -> io::Result<Segment> {
        let start_time = Instant::now();
        debug!("initializing {:?}", self);
        let segment = Segment::init_from_existing_file(&self.path, self.segment_num, partition_index)?;
        debug!("Finished initializing {:?} in {:?}", self, start_time.elapsed());
        Ok(segment)
    }
}

pub fn get_segment_files(partition_dir: &Path) -> io::Result<Vec<SegmentFile>> {
    let dir_entries = fs::read_dir(partition_dir)?;

    let mut segments = Vec::with_capacity(8);

    for entry in dir_entries {
        let dir_entry = entry?;
        if let Ok(string) = dir_entry.file_name().into_string() {
            if string.ends_with(DATA_FILE_EXTENSION) {
                let segment_num_str = string.split_terminator(DATA_FILE_EXTENSION).next();

                let segment_num = segment_num_str.and_then(|num_str| {
                    num_str.parse::<u64>().ok()
                }).ok_or_else(|| {
                    let message = format!("Invalid events filename: {}", string);
                    io::Error::new(io::ErrorKind::Other, message)
                })?; // early return if any of this business fails

                segments.push(SegmentFile {
                    path: dir_entry.path(),
                    segment_num: SegmentNum(segment_num),
                });
            }
        }
    }

    segments.sort_by_key(|sf| sf.segment_num);
    Ok(segments)
}

