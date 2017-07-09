mod event_writer;
mod counter_writer;

use byteorder::{ByteOrder, BigEndian};

use std::path::{PathBuf, Path};
use std::io::{self, Write, Read};
use std::fs::{self, create_dir_all};
use std::collections::{HashMap};
use std::ops::Add;

use engine::event_store::fs::{
    get_segment_directory,
    read_u64,
    determine_existing_actors
};

use chrono::Duration;
use event::{FloEvent, FloEventId, EventCounter, ActorId, VersionVector, Timestamp};

use self::event_writer::FileWriter;
use self::counter_writer::EventCounterWriter;

struct Writer {
    event_writer: FileWriter,
    counter_writer: EventCounterWriter,
}

impl Writer {
    fn write<E: FloEvent>(&mut self, event: &E) -> io::Result<u64> {
        self.event_writer.write(event).and_then(|event_offset| {
            self.counter_writer.commit(event.id().event_counter).map(|()| {
                event_offset
            })
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct SegmentStart {
    pub counter: EventCounter,
    pub time: Timestamp,
}

pub struct Segment {
    pub segment_start: SegmentStart,
    pub segment_duration: Duration,
    pub partition_number: u64,
    pub segment_end_time: Timestamp,
    segment_dir: PathBuf,
    writers_by_actor: HashMap<ActorId, Writer>,
}

impl Segment {
    /// Creates a segmentWriter. If the directory exists, then it will be initialized from data in that directory and the
    /// `start_time` parameter will be ignored. Otherwise, will create the directory and write the given `start_time`.
    pub fn initialize(storage_dir: &Path,
                      segment_number: u64,
                      default_start: SegmentStart,
                      segment_duration: Duration) -> io::Result<Segment> {
        let segment_dir = get_segment_directory(storage_dir, segment_number);
        debug!("initializing writer Segment: {}, default_start: {:?}, duration: {}", segment_number, default_start, segment_duration);
        create_dir_all(&segment_dir)?;

        // The presence of the segment_start file determines if there is an existing partition
        // It would be an error if there were existing events but no start_time file
        // TODO: we should validate that there are no existing actors and return an error if there are
        let existing_start_time = read_segment_start(&segment_dir)?;

        let (existing_actors, start) = if let Some(start) = existing_start_time {
            let actors = determine_existing_actors(&segment_dir)?;
            trace!("initializing segment: {} with start: {:?}, from existing data for actors: {:?}", segment_number, start, actors);
            (actors, start)
        } else {
            trace!("initializing segment: {} without existing data", segment_number);
            // Write the segment start time and return early if it fails
            write_segment_start(&segment_dir, &default_start)?;
            (Vec::new(), default_start)
        };

        let end_time = start.time.add(segment_duration);

        let mut writers_by_actor = HashMap::new();
        for actor_id in existing_actors {
            debug!("initializing writers for actor: {}, segment: {}", actor_id, segment_number);
            // Return early if either of these fail for any segment
            // TODO: use both counter and event writers to resolve any possible inconsistencies between the two files
            let counter_writer = EventCounterWriter::initialize(&segment_dir, segment_number, actor_id)?;
            let event_writer = FileWriter::initialize(&segment_dir, actor_id)?;
            let segment = Writer {
                event_writer: event_writer,
                counter_writer: counter_writer,
            };
            writers_by_actor.insert(actor_id, segment);
        }

        Ok(Segment {
            segment_start: start,
            segment_duration: segment_duration,
            partition_number: segment_number,
            segment_dir: segment_dir,
            writers_by_actor: writers_by_actor,
            segment_end_time: end_time,
        })
    }

    pub fn delete_segment(&mut self) -> io::Result<()> {
        info!("deleting segment: {} files at path: {:?}, segment_start: {:?}, segment_end_time: {}",
                self.partition_number,
                self.segment_dir,
                self.segment_start,
                self.segment_end_time);
        fs::remove_dir_all(&self.segment_dir)
    }

    pub fn create_version_vector(&self) -> VersionVector {
        let mut vv = VersionVector::new();
        for (actor, writer) in self.writers_by_actor.iter() {
            let id = FloEventId::new(*actor, writer.counter_writer.current_counter);
            vv.set(id);
        }
        vv
    }

    pub fn get_current_counter(&self, actor: ActorId) -> Option<EventCounter> {
        self.writers_by_actor.get(&actor).map(|writer| {
            writer.counter_writer.current_counter
        })
    }

    pub fn write<E: FloEvent>(&mut self, event: &E) -> Result<u64, io::Error> {
        let writer = self.get_or_create_writer(event.id().actor)?;
        writer.write(event)
    }

    fn get_or_create_writer(&mut self, actor: ActorId) -> Result<&mut Writer, io::Error> {
        if !self.writers_by_actor.contains_key(&actor) {
            let events_writer = FileWriter::initialize(&self.segment_dir, actor)?; //early return if this fails
            let counter_writer = EventCounterWriter::initialize(&self.segment_dir, self.partition_number, actor)?; //early return again

            self.writers_by_actor.insert(actor, Writer {
                event_writer: events_writer,
                counter_writer: counter_writer,
            });
        }
        Ok(self.writers_by_actor.get_mut(&actor).unwrap()) //safe unwrap since we just inserted the goddamn thing
    }
}


fn write_segment_start(segment_dir: &Path, start: &SegmentStart) -> Result<(), io::Error> {
    let mut info_file = open_segment_start_time(segment_dir, true)?;
    let mut buffer = [0u8; 16];
    BigEndian::write_u64(&mut buffer[0..8], ::event::time::millis_since_epoch(start.time));
    BigEndian::write_u64(&mut buffer[8..], start.counter);
    info_file.write_all(&buffer)
}

fn open_segment_start_time(segment_dir: &Path, create: bool) -> Result<fs::File, io::Error> {
    let path = segment_dir.join("segment_start");
    fs::OpenOptions::new().read(true).write(true).create(create).append(true).open(&path)
}


fn read_segment_start(segment_dir: &Path) -> Result<Option<SegmentStart>, io::Error> {
    let mut info_file = match open_segment_start_time(segment_dir, false) {
        Err(err) => {
            if err.kind() == io::ErrorKind::NotFound {
                return Ok(None);
            } else {
                return Err(err);
            }
        }
        Ok(file) => file
    };
    let time_u64 = read_u64(&mut info_file)?;
    let counter = read_u64(&mut info_file)?;
    let timestamp = ::event::time::from_millis_since_epoch(time_u64);
    Ok(Some(SegmentStart{
        time: timestamp,
        counter: counter
    }))
}
