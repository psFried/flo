
use std::sync::{Arc, RwLock};
use std::path::{PathBuf, Path};
use std::io::{self, Seek, Write, BufWriter};
use std::fs::{File, OpenOptions, create_dir_all};
use std::collections::{HashMap, VecDeque};

use engine::event_store::{EventWriter, StorageEngineOptions};
use engine::event_store::index::EventIndex;

use event::{FloEvent, FloEventId, ActorId, time};

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

struct PartitionWriter {
    partition_dir: PathBuf,
    writers_by_actor: HashMap<ActorId, FileWriter>,
    remaining_event_count: u64,
}

impl PartitionWriter {
    fn new(storage_dir: &Path, partition_number: u64, max_events_in_partition: u64) -> PartitionWriter {
        let partition_dir = super::get_partition_directory(storage_dir, partition_number);

        PartitionWriter {
            partition_dir: partition_dir,
            writers_by_actor: HashMap::new(),
            remaining_event_count: max_events_in_partition,
        }
    }

    fn write<E: FloEvent>(&mut self, event: &E) -> Result<u64, io::Error> {
        let result = {
            let writer = self.get_or_create_writer(event.id())?;
            writer.write(event)
        };
        if result.is_ok() {
            self.remaining_event_count -= 1;
        }
        result
    }

    fn get_or_create_writer(&mut self, id: &FloEventId) -> Result<&mut FileWriter, io::Error> {
        if !self.writers_by_actor.contains_key(&id.actor) {
            let writer = FileWriter::initialize(&self.partition_dir, id.actor)?; //early return if this fails
            self.writers_by_actor.insert(id.actor, writer);
        }
        Ok(self.writers_by_actor.get_mut(&id.actor).unwrap()) //safe unwrap since we just inserted the goddamn thing
    }
}

#[allow(dead_code)]
pub struct FSEventWriter {
    index: Arc<RwLock<EventIndex>>,
    storage_dir: PathBuf,
    partition_writers: HashMap<u64, PartitionWriter>,
    max_events: u64,
    partition_count: u64,
}

impl FSEventWriter {
    pub fn initialize(index: Arc<RwLock<EventIndex>>, storage_options: &StorageEngineOptions) -> Result<FSEventWriter, io::Error> {
        let mut storage_dir = storage_options.storage_dir.clone();
        create_dir_all(&storage_dir)?; //early return if this fails

        Ok(FSEventWriter{
            index: index,
            storage_dir: storage_dir,
            partition_writers: HashMap::new(),
            max_events: storage_options.max_events as u64,
            partition_count: super::PARTITION_COUNT,
        })
    }

    fn get_or_create_partition(&mut self, id: &FloEventId) -> &mut PartitionWriter {
        let partition_num = super::get_partition(self.partition_count, self.max_events, id.event_counter);
        if !self.partition_writers.contains_key(&partition_num) {
            let part_max = self.max_events / self.partition_count;
            let part_writer = PartitionWriter::new(&self.storage_dir, partition_num, part_max);
            self.partition_writers.insert(partition_num, part_writer);
        }
        self.partition_writers.get_mut(&partition_num).unwrap()
    }

}


impl EventWriter for FSEventWriter {
    type Error = io::Error;

    fn store<E: FloEvent>(&mut self, event: &E) -> Result<(), Self::Error> {
        use engine::event_store::index::IndexEntry;

        let write_result = {
            let writer = self.get_or_create_partition(event.id());
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
    BigEndian::write_u64(&mut buffer[32..40], time::millis_since_epoch(event.timestamp()));
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
