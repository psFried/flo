
use super::file_reader::FSEventIter;

use std::sync::{Arc, RwLock};
use std::io::{self, Seek, SeekFrom, Read, BufRead, BufReader};
use std::path::{PathBuf, Path};
use std::fs::{self, OpenOptions, File};
use std::collections::HashSet;
use std::iter::Peekable;

use byteorder::{ByteOrder, BigEndian};

use engine::event_store::index::{EventIndex, IndexEntry};
use engine::event_store::{EventReader, StorageEngineOptions};
use engine::event_store::fs::{DATA_FILE_EXTENSION, get_segment_directory, get_events_filename};
use event::{FloEventId, ActorId, OwnedFloEvent, Timestamp};


pub struct MultiSegmentReader {
    pub actor_id: ActorId,
    storage_dir: PathBuf,
    current_reader: FSEventIter,
    current_segment: u64,
    max: IndexEntry,
}

impl MultiSegmentReader {
    pub fn initialize(storage_dir: PathBuf, start: IndexEntry, end: IndexEntry) -> io::Result<MultiSegmentReader> {
        let first_segment = start.segment;
        let start_offset = start.offset;
        let max_id = end.id;
        let actor_id = start.id.actor;

        initialize_next_iter(&storage_dir, actor_id, first_segment, start_offset, max_id).map(|first_iter| {
            MultiSegmentReader {
                storage_dir: storage_dir,
                actor_id: actor_id,
                current_reader: first_iter,
                current_segment: first_segment,
                max: end,
            }
        })
    }

}

fn initialize_next_iter(storage_dir: &Path, actor_id: ActorId, segment: u64, offset: u64, max_id: FloEventId) -> io::Result<FSEventIter> {
    let mut path = get_segment_directory(storage_dir, segment);
    let filename = get_events_filename(actor_id);
    path.push(filename);

    FSEventIter::initialize(offset, max_id, &path, actor_id)
}

impl Iterator for MultiSegmentReader {
    type Item = Result<OwnedFloEvent, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.current_reader.next();

        if result.is_none() && self.current_segment < self.max.segment {
            debug!("initializing next segment reader for actor_id: {}, next segment: {}", self.actor_id, self.current_segment + 1);
            let new_reader = initialize_next_iter(&self.storage_dir, self.actor_id, self.current_segment + 1, 0, self.max.id);

            match new_reader {
                Ok(mut reader) => {
                    let _ = ::std::mem::swap(&mut self.current_reader, &mut reader);
                    self.current_segment += 1;
                    self.next()
                }
                Err(err) => {
                    Some(Err(err))
                }
            }
        } else {
           result
        }
    }
}








