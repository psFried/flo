pub mod file_reader;
mod multi_segment_reader;

use self::multi_segment_reader::MultiSegmentReader;
use self::file_reader::FSEventIter;

use std::sync::{Arc, RwLock};
use std::io;
use std::path::PathBuf;

use engine::event_store::fs::{
    determine_existing_partitions,
    determine_existing_actors,
    get_segment_directory,
    get_events_file,
    total_size_on_disk
};
use engine::event_store::index::{EventIndex, IndexEntry, ConsumerEntries};
use engine::event_store::{EventReader, StorageEngineOptions};
use event::{FloEventId, ActorId, VersionVector, OwnedFloEvent};

// We use this custom peekable iterator instead of the standard `iter.peekable()` method because this
// allows us to greedily advance the iterator and then peek at the next id without requiring a &mut
pub struct PeekableIterator {
    next_id: FloEventId,
    iter: MultiSegmentReader,
    next: Option<Result<OwnedFloEvent, io::Error>>,
}

impl PeekableIterator {
    pub fn new(iter: MultiSegmentReader) -> PeekableIterator {
        let mut wrapped_iter = PeekableIterator {
            iter: iter,
            next: None,
            next_id: FloEventId::zero()
        };
        let _ = wrapped_iter.advance();
        wrapped_iter
    }

    pub fn advance(&mut self) -> Option<Result<OwnedFloEvent, io::Error>> {
        let next = self.iter.next();
        let next_id = match next.as_ref() {
            Some(&Ok(ref event)) => event.id,
            Some(&Err(_)) => FloEventId::zero(),
            // if the iterator is exhausted, then we're going to return max id, so that this iterator just get's ignored
            None => FloEventId::max(),
        };
        trace!("Advancing file reader for actor: {}, prev id: {:?}, next_id: {:?}", self.actor_id(), self.next_id, next_id);
        self.next_id = next_id;
        ::std::mem::replace(&mut self.next, next)
    }

    pub fn actor_id(&self) -> ActorId {
        self.iter.actor_id
    }

    fn get_next_id(&self) -> FloEventId {
        self.next_id
    }

}

pub struct MultiActorEventIter{
    readers: Vec<PeekableIterator>,
    max_events: usize,
}

impl Iterator for MultiActorEventIter {
    type Item = Result<OwnedFloEvent, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let MultiActorEventIter {ref mut readers, ref mut max_events} = *self;
        if *max_events == 0 {
            debug!("returning None because iter has reached max number of events");
            return None;
        }

        let next_iter = readers.iter_mut().min_by_key(|iter| iter.get_next_id());

        let next_event = next_iter.and_then(|wrapped| {
            let next = wrapped.advance();

            if let Some(ref r) = next.as_ref() {
                let id = r.as_ref().map(|e| e.id);
                debug!("returning event: {:?} from {}", id, wrapped.actor_id());
            } else {
                debug!("returning None from {}", wrapped.actor_id());
            }
            next
        });

        if next_event.is_some() {
            *max_events -= 1;
        }
        next_event
    }
}


pub struct FSEventReader {
    index: Arc<RwLock<EventIndex>>,
    storage_dir: PathBuf,
}



impl FSEventReader {
    pub fn initialize(index: Arc<RwLock<EventIndex>>, options: &StorageEngineOptions) -> Result<FSEventReader, io::Error> {
        let storage_dir = options.storage_dir.to_owned();

        let mut reader = FSEventReader{
            index: index,
            storage_dir: storage_dir,
        };

        reader.init_index()?;

        Ok(reader)
    }

    fn init_index(&mut self) -> Result<(), io::Error> {
        let FSEventReader{ref mut index, ref mut storage_dir, ..} = *self;

        let mut locked_index = index.write().map_err(|lock_err| {
            io::Error::new(io::ErrorKind::Other, format!("failed to acquire write lock for index: {:?}", lock_err))
        })?;

        let existing_partitions = determine_existing_partitions(&storage_dir)?;
        for segment in existing_partitions {
            let segment_dir = get_segment_directory(&storage_dir, segment);
            let actors = determine_existing_actors(&segment_dir)?;
            for actor in actors {
                let path = get_events_file(&segment_dir, actor);
                let reader = FSEventIter::initialize(0, FloEventId::max(), &path, actor)?;
                let mut offset = 0;
                for event_result in reader {
                    let event = event_result?;
                    let entry = IndexEntry {
                        id: event.id,
                        offset: offset,
                        segment: segment,
                    };
                    locked_index.add(entry);
                    offset += total_size_on_disk(&event);
                }
            }
        }
        Ok(())
    }

}


impl EventReader for FSEventReader {
    type Iter = MultiActorEventIter;

    fn load_range(&mut self, range_start: &VersionVector, limit: usize) -> io::Result<Self::Iter> {
        let FSEventReader{ref mut index, ref mut storage_dir, ..} = *self;

        let index = index.read().expect("Unable to acquire read lock on event index");

        let start_iter = index.get_consumer_start_point(range_start).peekable();

        let mut readers: Vec<PeekableIterator> = Vec::with_capacity(8);

        for ConsumerEntries{start, end} in start_iter {
            let dir: PathBuf = storage_dir.to_owned();
            let reader = MultiSegmentReader::initialize(dir, start, end)?;

            readers.push(PeekableIterator::new(reader));
        }

        Ok(MultiActorEventIter{
            readers: readers,
            max_events: limit,
        })
    }
}



