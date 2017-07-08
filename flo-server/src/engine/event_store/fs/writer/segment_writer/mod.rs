mod event_writer;
mod counter_writer;


use std::sync::{Arc, RwLock};
use std::path::{PathBuf, Path};
use std::io::{self, Seek, Write, BufWriter};
use std::fs::{self, File, OpenOptions, create_dir_all};
use std::collections::{HashMap, VecDeque};
use std::ops::Add;

use engine::event_store::{EventWriter, StorageEngineOptions};
use engine::event_store::index::EventIndex;
use engine::event_store::fs::{
    get_segment_directory,
    DATA_FILE_EXTENSION,
    read_segment_start_time,
    write_segment_start_time,
    determine_existing_actors
};

use chrono::Duration;
use event::{FloEvent, FloEventId, EventCounter, ActorId, Timestamp, time};

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

pub struct Segment {
    pub segment_start_time: Timestamp,
    pub segment_duration: Duration,
    pub partition_number: u64,
    pub segment_end_time: Timestamp,
    segment_dir: PathBuf,
    writers_by_actor: HashMap<ActorId, Writer>,
}

impl Segment {
    /// Creates a segmentWriter. If the directory exists, then it will be initialized from data in that directory and the
    /// `start_time` parameter will be ignored. Otherwise, will create the directory and write the given `start_time`.
    pub fn initialize(storage_dir: &Path, segment_number: u64, default_start_time: Timestamp, segment_duration: Duration) -> io::Result<Segment> {
        let segment_dir = get_segment_directory(storage_dir, segment_number);
        debug!("initializing writer Segment: {}, default_start_time: {}, duration: {}", segment_number, default_start_time, segment_duration);
        create_dir_all(&segment_dir)?;

        // The presence of the segment_start file determines if there is an existing partition
        // It would be an error if there were existing events but no start_time file
        // TODO: we should validate that there are no existing actors and return an error if there are
        let existing_start_time = read_segment_start_time(&segment_dir)?;

        let (existing_actors, start_time) = if let Some(start) = existing_start_time {
            let actors = determine_existing_actors(&segment_dir)?;
            trace!("initializing segment: {} from existing data for actors: {:?}", segment_number, actors);
            (actors, start)
        } else {
            trace!("initializing segment: {} without existing data", segment_number);
            (Vec::new(), default_start_time)
        };

        let end_time = start_time.add(segment_duration);

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
            segment_start_time: start_time,
            segment_duration: segment_duration,
            partition_number: segment_number,
            segment_dir: segment_dir,
            writers_by_actor: writers_by_actor,
            segment_end_time: end_time,
        })
    }

    pub fn event_is_after_start(&self, id: FloEventId) -> bool {
        self.get_current_counter(id.actor) < id.event_counter
    }

    pub fn event_fits_within_time<E: FloEvent>(&self, event: &E) -> bool {
        self.get_current_counter(event.id().actor) < event.id().event_counter &&
                event.timestamp() < self.segment_end_time
    }

    pub fn get_current_counter(&self, actor: ActorId) -> EventCounter {
        self.writers_by_actor.get(&actor).map(|writer| {
            writer.counter_writer.current_counter
        }).unwrap_or(0)
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


