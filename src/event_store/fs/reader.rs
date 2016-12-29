use std::sync::{Arc, RwLock};
use std::io::{self, Seek, SeekFrom, Read, BufRead, BufReader};
use std::path::{PathBuf, Path};
use std::fs::{OpenOptions, File};

use byteorder::{ByteOrder, BigEndian};

use event_store::index::{EventIndex, IndexEntry};
use event_store::{EventReader, StorageEngineOptions};
use flo_event::{FloEventId, ActorId, EventCounter, OwnedFloEvent, FloEvent};

enum EventIterInner {
    Empty,
    NonEmpty{
        file_reader: BufReader<File>,
        remaining: usize,
        header_string: String,
    },
    Error(Option<io::Error>)
}

pub struct FSEventIter(EventIterInner);

impl FSEventIter {
    pub fn initialize(offset: u64, limit: usize, path: &Path) -> Result<FSEventIter, io::Error> {
        OpenOptions::new().create(true).read(true).write(false).open(path).and_then(|mut file| {
            file.seek(SeekFrom::Start(offset)).map(|_| {
                let inner = EventIterInner::NonEmpty {
                    file_reader: BufReader::new(file),
                    remaining: limit,
                    header_string: String::with_capacity(8),
                };
                FSEventIter(inner)
            })
        })
    }

    pub fn empty() -> FSEventIter {
        FSEventIter(EventIterInner::Empty)
    }

    fn error(err: io::Error) -> FSEventIter {
        FSEventIter(EventIterInner::Error(Some(err)))
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
            EventIterInner::NonEmpty {ref mut file_reader, ref mut remaining, ref mut header_string} => {
                if *remaining == 0 {
                    None
                } else {
                    *remaining -= 1;
                    Some(read_event(file_reader, header_string))
                }
            }
        }
    }
}

fn read_event<R: BufRead>(reader: &mut R, delimeter_buf: &mut String) -> Result<OwnedFloEvent, io::Error> {
    let mut buffer = [0; 26];

    reader.read_exact(&mut buffer[..]).and_then(|nread| {
        if &buffer[..8] == super::FLO_EVT.as_bytes() {
            let total_size = BigEndian::read_u32(&buffer[8..12]);
            let event_counter = BigEndian::read_u64(&buffer[12..20]);
            let actor_id = BigEndian::read_u16(&buffer[20..22]);
            let namespace_length = BigEndian::read_u32(&buffer[22..26]);

            // - 10 for the event id, -4 for the namespace length field, then minus the namespace, then the data length size;
            // used to validate data length for now while (de-)serialization still sucks
            let computed_data_length = total_size - 10 - 4 - namespace_length - 4;

            let mut namespace_buffer = Vec::with_capacity(namespace_length as usize);
            reader.read_exact(&mut namespace_buffer).and_then(move |ns_read| {
                String::from_utf8(namespace_buffer).map_err(|err| {
                    io::Error::new(io::ErrorKind::InvalidData, "namespace contained invalid utf8 character")
                })
            }).and_then(|namespace| {
                let mut data_len_buffer = [0; 4];
                reader.read_exact(&mut data_len_buffer).and_then(|()| {
                    let data_length = BigEndian::read_u32(&data_len_buffer);
                    debug_assert_eq!(data_length, computed_data_length);
                    let mut data_buffer = Vec::with_capacity(data_length as usize);
                    reader.read_exact(&mut data_buffer).map(|()| {
                        OwnedFloEvent::new(FloEventId::new(actor_id, event_counter), namespace, data_buffer)
                    })
                })
            })

        } else {
            invalid_bytes_err(format!("Expected {:?}, got: {:?}", super::FLO_EVT.as_bytes(), &buffer[..8]))
        }
    })
}

fn invalid_bytes_err(error_desc: String) -> Result<OwnedFloEvent, io::Error> {
    Err(io::Error::new(io::ErrorKind::InvalidData, error_desc))
}

pub struct FSEventReader {
    index: Arc<RwLock<EventIndex>>,
    storage_dir: PathBuf,
}

impl FSEventReader {
    pub fn initialize(index: Arc<RwLock<EventIndex>>, options: &StorageEngineOptions) -> Result<FSEventReader, io::Error> {
        let storage_dir = options.storage_dir.as_path().join(&options.root_namespace);
        Ok(FSEventReader{
            index: index,
            storage_dir: storage_dir,
        })
    }

    /// A bit of future proofing here, since index entry really isn't needed yet
    fn get_events_file(&self, entry: &IndexEntry) -> PathBuf {
        self.storage_dir.as_path().join(super::DATA_FILE_NAME)
    }
}

impl EventReader for FSEventReader {
    type Iter = FSEventIter;
    type Error = io::Error;

    fn load_range(&mut self, range_start: FloEventId, limit: usize) -> Self::Iter {
        let index = self.index.read().expect("Unable to acquire read lock on event index");

        index.get_next_entry(range_start).map(|entry| {
            let event_file = self.get_events_file(&entry);
            FSEventIter::initialize(entry.offset, limit, &event_file).unwrap_or_else(|err| {
                error!("Unable to create event iterator: {:?}", err);
                FSEventIter::error(err)
            })
        }).unwrap_or_else(|| {
            trace!("Creating an empty iterator in response to range_start: {:?}", range_start);
            FSEventIter::empty()
        })
    }

    fn get_highest_event_id(&mut self) -> FloEventId {
        let index = self.index.read().expect("Unable to acquire read lock on event index");
        index.get_greatest_event_id()
    }
}
