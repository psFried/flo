mod segment_writer;

use std::sync::{Arc, RwLock};
use std::path::PathBuf;
use std::io;
use std::fs::create_dir_all;
use std::collections::VecDeque;

use engine::event_store::{EventWriter, StorageEngineOptions};
use engine::event_store::index::EventIndex;
use engine::event_store::fs::determine_existing_partitions;

use chrono::Duration;
use event::{FloEvent, Timestamp};

use self::segment_writer::{Segment, SegmentStart};


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
        let event_retention_duration = storage_options.event_retention_duration;
        let segment_duration = storage_options.event_eviction_period;

        create_dir_all(&storage_dir)?; //early return if this fails
        let segment_numbers = determine_existing_partitions(&storage_dir)?;
        debug!("Initializing from existing segments: {:?}", segment_numbers);

        let segments_numbers_str = segment_numbers.iter().map(|n| n.to_string()).collect::<String>();

        let mut segment_writers = VecDeque::with_capacity(segment_numbers.len());

        for segment in segment_numbers {
            // The timestamp argument will be ignored since we know there's an existing segment start time persisted
            let ignored = SegmentStart{
                time: ::event::time::now(),
                counter: 1,
            };
            let writer = Segment::initialize(&storage_dir, segment, ignored, segment_duration)?;
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
        let create = self.check_most_recent_segment(event);

        if let CreateSegment::CreateNew {segment_number, start_time} = create {
            let duration = self.segment_duration;
            let segment_start = SegmentStart {
                counter: event.id().event_counter,
                time: start_time,
            };
            let new_segment = Segment::initialize(&self.storage_dir, segment_number, segment_start, duration)?;
            self.segment_writers.push_front(new_segment);
            Ok(self.segment_writers.front_mut().unwrap())
        } else {
            // there must be an existing segment to handle this
            //
            self.segment_writers.iter_mut().filter(|segment| {
                let event_counter = event.id().event_counter;
                let segment_start = &segment.segment_start;
                trace!("Checking to write event in segment: {}, event: {}, segment_start: {:?}, event_time: {}, segment_start: {:?}",
                        segment.partition_number,
                        event.id(),
                        segment_start,
                        event.timestamp(),
                        segment.segment_start);
                event_counter >= segment_start.counter || event.timestamp() >= segment_start.time
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

    fn purge_event_segments(&mut self, num_segments: usize) -> io::Result<()> {
        debug!("deleting the first {} segments", num_segments);
        for _ in 0..num_segments {
            if let Some(segment) = self.segment_writers.back_mut() {
                match segment.delete_segment() {
                    Ok(()) => {
                        debug!("Successfully deleted segment: {}", segment.partition_number);
                    }
                    Err(io_err) => {
                        error!("Error deleting segment: {} - {:?}", segment.partition_number, io_err);
                        return Err(io_err);
                    }
                }
            } else {
                //There's no more segments to purge
                debug!("No more segments to purge");
                return Ok(());
            }

            // now attempt to remove the segment after it's been successfully deleted
            if let Some(segment) = self.segment_writers.pop_back() {
                self.delete_index_entries(segment)?;
            }
        }
        Ok(())
    }

    fn delete_index_entries(&mut self, segment: Segment) -> io::Result<()> {
        trace!("deleting index entries for segment: {}", segment.partition_number);
        let mut locked_index = self.index.write().map_err(|lock_err| {
            io::Error::new(io::ErrorKind::Other, format!("Error locking index: {:?}", lock_err))
        })?; // return early if lock fails

        let segment_version_vector = segment.create_version_vector();
        locked_index.remove_range_inclusive(&segment_version_vector);
        Ok(())
    }
}

fn check_last_segment<E: FloEvent>(event: &E, last_segment: &Segment) -> CreateSegment {
    let id = *event.id();
    let segment_counter = last_segment.get_current_counter(id.actor).unwrap_or(0);

    let is_after = id.event_counter > segment_counter;

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
            trace!("Writing event: {}, timestamp: {} into segment: {}, segment_start: {:?}", event.id(), event.timestamp(), writer.partition_number, writer.segment_start);
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

    fn expire_events_before(&mut self, threshold: Timestamp) -> Result<(), io::Error> {
        let retained_segment_index = self.segment_writers.iter().rev().position(|segment| {
            trace!("checking segment: {}, segment_start: {:?}, threshold: {:?}", segment.partition_number, segment.segment_start, threshold);
            segment.segment_start.time > threshold
        }).map(|idx| {
            // idx is the index of the first segment that has a start time > the threshold
            // The segment before this one can still contain events that are before the threshold, though, so we sub 1
            idx.saturating_sub(1)
        });

        /*
        If we couldn't find ANY segments that were started after the threshold, then we'll purge all of them except for
        the most recent one. The most recent one _could_ contain events that should be retained.
        */
        let num_to_purge = retained_segment_index.unwrap_or(self.segment_writers.len() - 1);
        self.purge_event_segments(num_to_purge)
    }
}


