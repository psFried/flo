
use std::sync::{Arc, RwLock};
use std::path::PathBuf;
use std::io::{self, Seek, Write, BufWriter};
use std::fs::{File, OpenOptions, create_dir_all};

use event_store::{EventWriter, StorageEngineOptions};
use event_store::index::EventIndex;

use flo_event::{FloEventId, ActorId, EventCounter, FloEvent, OwnedFloEvent};

pub struct FSEventWriter {
    index: Arc<RwLock<EventIndex>>,
    storage_dir: PathBuf,
    event_writer: BufWriter<File>,
    offset: u64,
}

impl FSEventWriter {
    pub fn initialize(index: Arc<RwLock<EventIndex>>, storage_options: &StorageEngineOptions) -> Result<FSEventWriter, io::Error> {
        let mut storage_dir = storage_options.storage_dir.clone();
        storage_dir.push(&storage_options.root_namespace);
        create_dir_all(&storage_dir)?;

        let events_file = storage_dir.as_path().join(super::DATA_FILE_NAME);
        let open_result = OpenOptions::new().write(true).truncate(false).create(true).append(true).open(&events_file);

        open_result.and_then(|mut file| {
            file.seek(io::SeekFrom::End(0)).map(|file_offset| {
                trace!("initializing writer starting at offset: {}", file_offset);
                let writer = BufWriter::new(file);

                FSEventWriter {
                    offset: file_offset,
                    index: index,
                    storage_dir: storage_dir,
                    event_writer: writer,
                }
            })
        })
    }
}

impl EventWriter for FSEventWriter {
    type Error = io::Error;

    fn store<E: FloEvent>(&mut self, event: &E) -> Result<(), Self::Error> {
        use event_store::index::IndexEntry;

        let FSEventWriter{ref mut offset, ref mut index, ref mut event_writer, ..} = *self;

        write_event(event_writer, event).map(|size_on_disk| {
            trace!("Wrote event: {:?} to disk as: {} bytes, starting at offset: {}", event.id(), size_on_disk, offset);
            index.write().map(|mut idx| {
                let index_entry = IndexEntry::new(*event.id(), *offset);
                let evicted = idx.add(index_entry);
                *offset += size_on_disk;
                if let Some(removed_event) = evicted {
                    debug!("Evicted old event: {:?}", removed_event.id);
                }
            });
            ()
        })
    }
}

pub fn write_event<W: Write, E: FloEvent>(writer: &mut W, event: &E) -> Result<u64, io::Error> {
    use byteorder::{ByteOrder, BigEndian};

    //initialize buffer with FLO_EVT\n as first 8 characters and leave enough room to write event id, data length
    let mut buffer = [70, 76, 79, 95, 69, 86, 84, 10, //FLO_EVT\n
            0,0,0,0,                                  //total length (4 bytes for u32)
            0,0,0,0,0,0,0,0,0,0,                      //event id     (10 bytes for u64 and u16)
            0,0,0,0,                                  //namespace length (4 bytes for u32)
    ];

    let size_on_disk = (buffer.len() + //includes the FLO_EVT\n header
            event.namespace().len() +  // length of namespace
            4 +                        // 4 bytes for event data length (u32)
            event.data_len() as usize  // length of event data
    ) as u64;


    BigEndian::write_u32(&mut buffer[8..12], size_on_disk as u32 - 8); //subtract 8 because the total size includes the FLO_EVT\n tag
    BigEndian::write_u64(&mut buffer[12..20], event.id().event_counter);
    BigEndian::write_u16(&mut buffer[20..22], event.id().actor);
    BigEndian::write_u32(&mut buffer[22..26], event.namespace().len() as u32);

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
