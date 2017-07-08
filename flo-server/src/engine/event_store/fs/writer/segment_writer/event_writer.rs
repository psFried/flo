use std::sync::{Arc, RwLock};
use std::path::{PathBuf, Path};
use std::io::{self, Seek, Write, BufWriter};
use std::fs::{File, OpenOptions, create_dir_all};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use engine::event_store::{EventWriter, StorageEngineOptions};
use engine::event_store::index::EventIndex;
use engine::event_store::fs::get_events_file;

use event::{FloEvent, FloEventId, EventCounter, ActorId, Timestamp, time};


pub struct FileWriter {
    _actor_id: ActorId,
    events_writer: BufWriter<File>,
    current_offset: u64,
}

impl FileWriter {
    pub fn initialize(partition_dir: &Path, actor: ActorId) -> Result<FileWriter, io::Error> {
        // First open the events file
        let events_file_path = get_events_file(partition_dir, actor);
        let mut events_file = OpenOptions::new().write(true).truncate(false).create(true).append(true).open(&events_file_path)?;
        let events_file_offset = events_file.seek(io::SeekFrom::End(0))?;

        trace!("initializing writer for actor: {} starting at offset: {} in file: {:?}", actor, events_file_offset, &events_file_path);
        let events_writer = BufWriter::new(events_file);

        Ok(FileWriter{
            _actor_id: actor,
            events_writer: events_writer,
            current_offset: events_file_offset,
        })
    }

    pub fn write<E: FloEvent>(&mut self, event: &E) -> Result<u64, io::Error> {
        let size_on_disk = write_event(&mut self.events_writer, event)?;
        let event_offset = self.current_offset;
        debug!("wrote event to disk for actor: {}, start_offset: {}, event_size_in_bytes: {}", event.id().actor, event_offset, size_on_disk);
        self.current_offset += size_on_disk;
        Ok(event_offset)
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

