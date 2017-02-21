
use std::sync::{Arc, RwLock};
use std::path::{PathBuf, Path};
use std::io::{self, Seek, Write, BufWriter};
use std::fs::{File, OpenOptions, create_dir_all};
use std::collections::HashMap;

use engine::event_store::{EventWriter, StorageEngineOptions};
use engine::event_store::index::EventIndex;

use event::{FloEvent, ActorId};

//Actor and file_path are here so we can re-open files later
struct FileWriter {
    _actor_id: ActorId,
    _file_path: PathBuf,
    writer: BufWriter<File>,
    current_offset: u64,
}

impl FileWriter {
    fn initialize(storage_dir: &Path, actor: ActorId) -> Result<FileWriter, io::Error> {
        let events_file = super::get_events_file(storage_dir, actor);
        let open_result = OpenOptions::new().write(true).truncate(false).create(true).append(true).open(&events_file);

        open_result.and_then(|mut file| {
            file.seek(io::SeekFrom::End(0)).map(|file_offset| {
                trace!("initializing writer for actor: {} starting at offset: {} in file: {:?}", actor, file_offset, &events_file);
                let writer = BufWriter::new(file);

                FileWriter{
                    _actor_id: actor,
                    _file_path: events_file,
                    writer: writer,
                    current_offset: file_offset,
                }
            })
        })
    }

    fn write<E: FloEvent>(&mut self, event: &E) -> Result<u64, io::Error> {
        let size_on_disk = write_event(&mut self.writer, event)?;
        let event_offset = self.current_offset;
        debug!("wrote event to disk for actor: {}, start_offset: {}, event_size_in_bytes: {}", event.id().actor, event_offset, size_on_disk);
        self.current_offset += size_on_disk;
        Ok(event_offset)
    }
}

#[allow(dead_code)]
pub struct FSEventWriter {
    index: Arc<RwLock<EventIndex>>,
    storage_dir: PathBuf,
    writers_by_actor: HashMap<ActorId, FileWriter>,
}

impl FSEventWriter {
    pub fn initialize(index: Arc<RwLock<EventIndex>>, storage_options: &StorageEngineOptions) -> Result<FSEventWriter, io::Error> {
        let mut storage_dir = storage_options.storage_dir.clone();
        storage_dir.push(&storage_options.root_namespace);
        create_dir_all(&storage_dir)?; //early return if this fails

        let mut file_writers = HashMap::new();
        let max_actor_id = {
            //TODO: how safe is it to trust that the index has complete knowledge of all actors at this point in initialization?
            let idx = index.read().map_err(|err| {
                io::Error::new(io::ErrorKind::Other, format!("Error acquiring write lock for index: {:?}", err))
            })?;
            idx.get_max_actor_id()
            //drop the read lock at the end of this block
        };
        for actor_id in 0..max_actor_id {
            //Open all the events files up front, NOT lazily. Early return if any fails
            let writer = FileWriter::initialize(&storage_dir, actor_id)?;
            file_writers.insert(actor_id, writer);
        }
        Ok(FSEventWriter{
            index: index,
            storage_dir: storage_dir,
            writers_by_actor: file_writers,
        })
    }

    fn get_or_create_writer(&mut self, actor: ActorId) -> Result<&mut FileWriter, io::Error> {
        if !self.writers_by_actor.contains_key(&actor) {
            let writer = FileWriter::initialize(&self.storage_dir, actor)?; //early return if this fails
            self.writers_by_actor.insert(actor, writer);
        }
        Ok(self.writers_by_actor.get_mut(&actor).unwrap()) //safe unwrap since we just inserted the goddamn thing
    }
}


impl EventWriter for FSEventWriter {
    type Error = io::Error;

    fn store<E: FloEvent>(&mut self, event: &E) -> Result<(), Self::Error> {
        use engine::event_store::index::IndexEntry;

        let write_result = {
            let writer = self.get_or_create_writer(event.id().actor)?; //early return if this fails
            writer.write(event)
        };
        write_result.and_then(|event_offset| {
            self.index.write().map_err(|err| {
                io::Error::new(io::ErrorKind::Other, format!("Error acquiring write lock for index: {:?}", err))
            }).map(|mut idx| {
                let index_entry = IndexEntry::new(*event.id(), event_offset);
                let evicted = idx.add(index_entry);
                if let Some(removed_event) = evicted {
                    debug!("Evicted old event: {:?}", removed_event.id);
                }
            })
        })
    }
}

pub fn write_event<W: Write, E: FloEvent>(writer: &mut W, event: &E) -> Result<u64, io::Error> {
    use byteorder::{ByteOrder, BigEndian};

    //initialize buffer with FLO_EVT\n as first 8 characters and leave enough room to write event id, data length
    let mut buffer = [70, 76, 79, 95, 69, 86, 84, 10, //FLO_EVT\n
            0,0,0,0,                                  //total length (4 bytes for u32)
            0,0,0,0,0,0,0,0,0,0,                      //event id     (10 bytes for u64 and u16)
            0,0,0,0,0,0,0,0,0,0,                      //parent event id     (10 bytes for u64 and u16)
            0,0,0,0,0,0,0,0,                          //timestamp       (8 bytes for u64 millis since unix epoch)
            0,0,0,0,                                  //namespace length (4 bytes for u32)
    ];

    let size_on_disk = (buffer.len() + //includes the FLO_EVT\n header
            event.namespace().len() +  // length of namespace
            4 +                        // 4 bytes for event data length (u32)
            event.data_len() as usize  // length of event data
    ) as u64;

    let (parent_counter, parent_actor) = event.parent_id().map(|id| {
        (id.event_counter, id.actor)
    }).unwrap_or((0, 0));

    BigEndian::write_u32(&mut buffer[8..12], size_on_disk as u32 - 8); //subtract 8 because the total size includes the FLO_EVT\n tag
    BigEndian::write_u64(&mut buffer[12..20], event.id().event_counter);
    BigEndian::write_u16(&mut buffer[20..22], event.id().actor);
    BigEndian::write_u64(&mut buffer[22..30], parent_counter);
    BigEndian::write_u16(&mut buffer[30..32], parent_actor);
    BigEndian::write_u64(&mut buffer[32..40], ::time::millis_since_epoch(event.timestamp()));
    BigEndian::write_u32(&mut buffer[40..44], event.namespace().len() as u32);

    writer.write(&buffer).and_then(|_| {
        writer.write(event.namespace().as_bytes()).and_then(|_| {
            let mut data_len_buffer = [0; 4];
            BigEndian::write_u32(&mut data_len_buffer[..], event.data_len());
            writer.write(&data_len_buffer).and_then(|_| {
                writer.write(event.data())
            })
        })
    }).and_then(|_| {
        writer.flush()
    }).map(|_| {
        size_on_disk
    })
}
