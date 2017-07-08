mod segment_writer;

use std::sync::{Arc, RwLock};
use std::path::{PathBuf, Path};
use std::io::{self, Seek, Write, BufWriter};
use std::fs::{File, OpenOptions, create_dir_all};
use std::collections::{HashMap, VecDeque};

use engine::event_store::{EventWriter, StorageEngineOptions};
use engine::event_store::index::EventIndex;
use engine::event_store::fs::{determine_existing_partitions, determine_existing_actors};

use chrono::Duration;
use event::{FloEvent, FloEventId, EventCounter, ActorId, Timestamp, time};

use self::segment_writer::Segment;


#[allow(dead_code)]
pub struct FSEventWriter {
    index: Arc<RwLock<EventIndex>>,
    storage_dir: PathBuf,
    segment_writers: VecDeque<Segment>,
    segment_duration: Duration,
    event_expiration_duration: Duration,
}


/*

Each SegmentWriter keeps its starting time bound, as well as the version vector for JUST THAT SEGMENT
SegmentWriters are stored in a DeQueue sorted with earliest segment at the beginning, and the most recent segment at the end

to write an event:
    - Iterate the DeQueue from newest to oldest
        - If the SegmentWriter start_time is less than the event's timestamp, AND the event's id is greater than what's in the SegmentWriter's version vector for that actor
            - If this is the most recent SegmentWriter, then check the event timestamp against the SegmentWriter.start_time + SegmentWriter.duration
                - If it fits, then write into that segment
                - Otherwise, it's outside the time range for that segment, so we need to create a new SegmentWriter
                    - Create a new SegmentWriter with the next segment number,
                    - the start_time should be = previous_segment.start_time + segment_duration
                    - Write the event into the new SegmentWriter
        - Otherwise:
            - Keep iterating in reverse
        - reaching the beginning of the DeQueue is an Error!

to expire old events, given a retention_timestamp that marks the threshold of events we want to keep:
    - Iterate the DeQueue from oldest to newest:
        - Find the index, 'retained_segments_index' of the LAST SegmentWriter where the start time bound is less than the retention_timestamp
        - Then drop that NUMBER of segments from the DeQueue (we want the shift by 1 here, because it will cause the element at retained_segments_index to be retained)
            - Iterate these from earliest to latest:
                - Remove the SegmentWriter from the DeQueue
                - Delete the whole directory for the segment


to initialize a SegmentWriter from disk:
    - read the starting timestamp from disk
    - read the version vector from disk
    - The Duration of events that go into the most recent segmentWriter may be different upon startup than it was previously

*/

impl FSEventWriter {
    pub fn initialize(index: Arc<RwLock<EventIndex>>, storage_options: &StorageEngineOptions) -> Result<FSEventWriter, io::Error> {
        let storage_dir = storage_options.storage_dir.clone();
        //TODO: these values from StorageEngineOptions
        let event_retention_duration = Duration::days(30);
        let segment_duration = Duration::days(2);

        create_dir_all(&storage_dir)?; //early return if this fails
        let segment_numbers = determine_existing_partitions(&storage_dir)?;
        debug!("Initializing from existing segments: {:?}", segment_numbers);

        let segments_numbers_str = segment_numbers.iter().map(|n| n.to_string()).collect::<String>();

        let mut segment_writers = VecDeque::with_capacity(segment_numbers.len());

        for segment in segment_numbers {
            let timestamp = ::event::time::now();
            let writer = Segment::initialize(&storage_dir, segment, timestamp, event_retention_duration)?;
            segment_writers.push_front(writer);
        }

        let writers_str = segment_writers.iter().map(|w| w.partition_number.to_string()).collect::<String>();
        println!("initialized from segment numbers: [{}], segments: [{}]", segments_numbers_str, writers_str);

        Ok(FSEventWriter {
            index: index,
            storage_dir: storage_dir,
            segment_writers: segment_writers,
            segment_duration: segment_duration,
            event_expiration_duration: event_retention_duration,

        })
    }

    fn get_or_create_segment<E: FloEvent>(&mut self, event: &E) -> io::Result<&mut Segment> {
        let event_time = event.timestamp();
        let event_id = *event.id();

        let create = self.check_most_recent_segment(event);

        if let CreateSegment::CreateNew {segment_number, start_time} = create {
            let duration = self.segment_duration;
            let new_segment = Segment::initialize(&self.storage_dir, segment_number, start_time, duration)?;
            self.segment_writers.push_front(new_segment);
            Ok(self.segment_writers.front_mut().unwrap())
        } else {
            // there must be an existing segment to handle this
            self.segment_writers.iter_mut().filter(|segment| {
                true
            }).next()
                .ok_or_else(|| {
                    let message = format!("No valid segment exists for event: {}, with timestamp: {}", event.id(), event.timestamp());
                    io::Error::new(io::ErrorKind::Other, message)
                })
        }
    }


    fn check_most_recent_segment<E: FloEvent>(&self, event: &E) -> CreateSegment {
        match self.segment_writers.front() {
            Some(last_segment) => {
                check_last_segment(event, last_segment)
            }
            None => {
                // if segment_writers is empty, then create the first segment
                CreateSegment::CreateNew {
                    segment_number: 1,
                    start_time: event.timestamp(),
                }
            }
        }
    }

}

fn check_last_segment<E: FloEvent>(event: &E, last_segment: &Segment) -> CreateSegment {
    let id = *event.id();
    let is_after = id.event_counter > last_segment.get_current_counter(id.actor);

    if is_after && event.timestamp() > last_segment.segment_end_time {
        // the event should be written into a new segment, since it falls after the last segment
        CreateSegment::CreateNew {
            segment_number: last_segment.partition_number + 1,
            start_time: last_segment.segment_end_time,
        }
    } else {
        // Either the event fits into the last segment, or it belongs in an earlier segment
        CreateSegment::UseExisting
    }
}

#[derive(PartialEq, Debug, Clone)]
enum CreateSegment {
    CreateNew{
        segment_number: u64,
        start_time: Timestamp,
    },
    UseExisting
}


impl EventWriter for FSEventWriter {

    fn store<E: FloEvent>(&mut self, event: &E) -> io::Result<()> {
        use engine::event_store::index::IndexEntry;

        let (write_result, partition_number) = {
            let writer = self.get_or_create_segment(event)?;
            let segment_number = writer.partition_number;
            let result = writer.write(event);
            (result, segment_number)
        };

        write_result.and_then(|event_offset| {
            self.index.write().map_err(|err| {
                io::Error::new(io::ErrorKind::Other, format!("Error acquiring write lock for index: {:?}", err))
            }).map(|mut idx| {
                let index_entry = IndexEntry::new(*event.id(), event_offset, partition_number);
                idx.add(index_entry);
            })
        })
    }
}


