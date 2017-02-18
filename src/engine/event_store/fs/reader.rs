use std::sync::{Arc, RwLock};
use std::io::{self, Seek, SeekFrom, Read, BufRead, BufReader};
use std::path::{PathBuf, Path};
use std::fs::{self, OpenOptions, File};
use std::collections::HashSet;

use byteorder::{ByteOrder, BigEndian};

use engine::event_store::index::{EventIndex, IndexEntry};
use engine::event_store::{EventReader, StorageEngineOptions};
use flo_event::{FloEventId, ActorId, OwnedFloEvent, Timestamp};

enum EventIterInner {
    Empty,
    NonEmpty{
        file_reader: BufReader<File>,
        remaining: usize,
    },
    Error(Option<io::Error>)
}

struct WrappedIterator {
    iter: FSEventIter,
    next: Option<Result<OwnedFloEvent, io::Error>>,
    next_id: FloEventId,
}

impl WrappedIterator {
    fn new(mut iter: FSEventIter) -> WrappedIterator {
        let mut wrapped_iter = WrappedIterator {
            iter: iter,
            next: None,
            next_id: FloEventId::zero(),
        };
        let _ = wrapped_iter.advance();
        wrapped_iter
    }

    fn advance(&mut self) -> Option<Result<OwnedFloEvent, io::Error>> {
        let next = self.iter.next();
        let next_id = match &next {
            &Some(Ok(ref event)) => event.id,
            _ => FloEventId::zero()
        };
        trace!("Advancing file reader for actor: {}, prev id: {:?}, next_id: {:?}", self.actor_id(), self.next_id, next_id);
        self.next_id = next_id;
        ::std::mem::replace(&mut self.next, next)
    }

    fn actor_id(&self) -> ActorId {
        self.iter.1
    }
}

pub struct MultiActorEventIter {
    iters: Vec<WrappedIterator>
}

impl Iterator for MultiActorEventIter {
    type Item = Result<OwnedFloEvent, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_iter: Option<&mut WrappedIterator> = self.iters.iter_mut().min_by_key(|wrapped| wrapped.next_id);
        next_iter.and_then(|wrapped| wrapped.advance())
    }
}

pub struct FSEventIter(EventIterInner, ActorId);

impl FSEventIter {
    pub fn initialize(offset: u64, limit: usize, path: &Path, actor_id: ActorId) -> Result<FSEventIter, io::Error> {
        trace!("opening file at path: {:?}", path);
        OpenOptions::new().read(true).open(path).and_then(|mut file| {
            file.seek(SeekFrom::Start(offset)).map(|_| {
                let inner = EventIterInner::NonEmpty {
                    file_reader: BufReader::new(file),
                    remaining: limit,
                };
                FSEventIter(inner, actor_id)
            })
        })
    }

    pub fn empty(actor_id: ActorId) -> FSEventIter {
        FSEventIter(EventIterInner::Empty, actor_id)
    }

    fn error(err: io::Error, actor_id: ActorId) -> FSEventIter {
        FSEventIter(EventIterInner::Error(Some(err)), actor_id)
    }
}

impl Iterator for FSEventIter {
    type Item = Result<OwnedFloEvent, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0 {
            EventIterInner::Empty => None,
            EventIterInner::Error(ref mut err) => {
                err.take().map(|e| {
                    Err(e)
                })
            }
            EventIterInner::NonEmpty {ref mut file_reader, ref mut remaining} => {
                if *remaining == 0 {
                    None
                } else {
                    *remaining -= 1;
                    match has_next_event(file_reader) {
                        Ok(true) => {
                            Some(read_event(file_reader))
                        }
                        Ok(false) => {
                            None
                        }
                        Err(err) => {
                            Some(Err(err))
                        }
                    }
                }
            }
        }
    }
}

fn has_next_event<R: BufRead>(reader: &mut R) -> Result<bool, io::Error> {
    reader.fill_buf().map(|buffer| {
        buffer.len() >= 8 && &buffer[..8] == super::FLO_EVT.as_bytes()
    })
}

pub struct EventHeader {
    pub total_size: u32,
    pub event_counter: u64,
    pub actor_id: u16,
    pub parent_counter: u64,
    pub parent_actor_id: u16,
    pub timestamp: u64,
    pub namespace_length: u32,
}

impl EventHeader {

    pub fn compute_data_length(&self) -> usize {
        /*
        -4 bytes for the total size field
        -10 for event id
        -10 for parent event id
        -8 for timestamp
        -4 for namespace length field
        - the namespace itself
        -4 for the data length field
        */
        (self.total_size - 4 - 10 - 10 - 8 - 4 - self.namespace_length - 4) as usize
    }

    pub fn event_id(&self) -> FloEventId {
        FloEventId::new(self.actor_id, self.event_counter)
    }

    pub fn parent_id(&self) -> Option<FloEventId> {
        if self.parent_counter > 0 {
            Some(FloEventId::new(self.parent_actor_id, self.parent_counter))
        } else {
            None
        }
    }

    pub fn timestamp(&self) -> Timestamp {
        ::time::from_millis_since_epoch(self.timestamp)
    }
}

pub fn read_header<R: Read>(reader: &mut R) -> Result<EventHeader, io::Error> {
    let mut buffer = [0; 44];
    reader.read_exact(&mut buffer[..])?;

    if &buffer[..8] == super::FLO_EVT.as_bytes() {
        Ok(EventHeader{
            total_size: BigEndian::read_u32(&buffer[8..12]),
            event_counter: BigEndian::read_u64(&buffer[12..20]),
            actor_id: BigEndian::read_u16(&buffer[20..22]),
            parent_counter: BigEndian::read_u64(&buffer[22..30]),
            parent_actor_id: BigEndian::read_u16(&buffer[30..32]),
            timestamp: BigEndian::read_u64(&buffer[32..40]),
            namespace_length: BigEndian::read_u32(&buffer[40..44]),
        })
    } else {
        Err(invalid_bytes_err(format!("expected {:?}, got: {:?}", super::FLO_EVT.as_bytes(), &buffer[..8])))
    }
}

pub fn read_event<R: Read>(reader: &mut R) -> Result<OwnedFloEvent, io::Error> {
    read_header(reader).and_then(|header| {
        let mut namespace_buffer = vec![0; header.namespace_length as usize];
        reader.read_exact(&mut namespace_buffer).and_then(move |_ns_read| {
            String::from_utf8(namespace_buffer).map_err(|err| {
                invalid_bytes_err(format!("namespace contained invalid utf8 character: {:?}", err))
            })
        }).and_then(|namespace| {
            let mut data_len_buffer = [0; 4];
            reader.read_exact(&mut data_len_buffer).and_then(|()| {
                let data_length = BigEndian::read_u32(&data_len_buffer);
                debug_assert_eq!(data_length as usize, header.compute_data_length());
                let mut data_buffer = vec![0; data_length as usize];
                reader.read_exact(&mut data_buffer).map(|()| {
                    OwnedFloEvent::new(header.event_id(), header.parent_id(), header.timestamp(), namespace, data_buffer)
                })
            })
        })
    })
}

fn invalid_bytes_err(error_desc: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error_desc)
}

pub struct FSEventReader {
    index: Arc<RwLock<EventIndex>>,
    storage_dir: PathBuf,
    existing_actors: HashSet<ActorId>,
}

fn determine_existing_actors(storage_dir: &Path) -> Result<HashSet<ActorId>, io::Error> {
    let mut actors = HashSet::new();
    for entry in fs::read_dir(storage_dir)? {
        let filename_os_str = entry?.file_name();
        let filename = filename_os_str.to_string_lossy();
        if let Some(actor_num_str) = filename.split_terminator(super::DATA_FILE_EXTENSION).next() {
            debug!("Found events file for actor: '{}'", actor_num_str);
            let id = actor_num_str.parse::<ActorId>().map_err(|parse_err| {
                io::Error::new(io::ErrorKind::InvalidData, format!("The filename '{}' is invalid", filename))
            })?; // early return if the parse goes awry
            actors.insert(id);
        }
    }
    Ok(actors)
}

impl FSEventReader {
    pub fn initialize(index: Arc<RwLock<EventIndex>>, options: &StorageEngineOptions) -> Result<FSEventReader, io::Error> {
        let storage_dir = options.storage_dir.as_path().join(&options.root_namespace);

        let existing_actors = determine_existing_actors(&storage_dir)?;

        let mut reader = FSEventReader{
            index: index,
            storage_dir: storage_dir,
            existing_actors: existing_actors,
        };

        reader.init_index()?;

        Ok(reader)
    }

    fn init_index(&mut self) -> Result<(), io::Error> {
        let FSEventReader{ref mut index, ref mut storage_dir, ref mut existing_actors} = *self;

        let mut locked_index = index.write().map_err(|lock_err| {
            io::Error::new(io::ErrorKind::Other, format!("failed to acquire write lock for index: {:?}", lock_err))
        })?;
        for &actor_id in existing_actors.iter() {
            debug!("Adding events to index from actor: {}", actor_id);
            let events_file = get_events_file(&storage_dir, actor_id);
            let mut iter = FSEventIter::initialize(0, ::std::usize::MAX, &events_file, actor_id)?;

            let mut num_events_for_actor = 0;
            let mut offset = 0;
            for read_result in iter {
                let event = read_result?;
                let entry = IndexEntry::new(event.id, offset);
                locked_index.add(entry); //don't care about evictions here
                offset += super::total_size_on_disk(&event);
                num_events_for_actor += 1;
            }
            debug!("Finished adding {} events to index for actor: {}", num_events_for_actor, actor_id);
        }
        info!("finished building index with {} total entries", locked_index.entry_count());
        Ok(())
    }

}

fn get_events_file(storage_dir: &Path, actor_id: ActorId) -> PathBuf {
    storage_dir.join(format!("{}{}", actor_id, super::DATA_FILE_EXTENSION))
}

impl EventReader for FSEventReader {
    type Iter = MultiActorEventIter;
    type Error = io::Error;

    fn load_range(&mut self, range_start: FloEventId, limit: usize) -> Self::Iter {
        let FSEventReader{ref mut index, ref mut storage_dir, ref mut existing_actors} = *self;

        let index = index.read().expect("Unable to acquire read lock on event index");
        let versions = index.get_version_vector().snapshot();

        let iters = versions.iter().map(|id| {
            let actor_id = id.actor;
            index.get_next_entry_for_actor(range_start, actor_id).map(|entry| {
                let event_file = get_events_file(&storage_dir, entry.id.actor);
                trace!("range_start: {:?}, next_entry: {:?}, file: {:?}", range_start, entry, event_file);
                FSEventIter::initialize(entry.offset, limit, &event_file, actor_id).unwrap_or_else(|err| {
                    error!("Unable to create event iterator: {:?}", err);
                    FSEventIter::error(err, actor_id)
                })
            }).unwrap_or_else(|| {
                trace!("Creating an empty iterator in response to range_start: {:?}", range_start);
                FSEventIter::empty(actor_id)
            })
        }).map(|fs_iter| {
            WrappedIterator::new(fs_iter)
        }).collect::<Vec<WrappedIterator>>();

        MultiActorEventIter{
            iters: iters,
        }
    }

    fn get_highest_event_id(&mut self) -> FloEventId {
        let index = self.index.read().expect("Unable to acquire read lock on event index");
        index.get_greatest_event_id()
    }
}
