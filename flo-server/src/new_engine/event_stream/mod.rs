pub mod partition;

use std::path::{PathBuf, Path};
use std::io;

use chrono::Duration;

use event_loops::LoopHandles;
use event::ActorId;
use self::partition::{PartitionRef, PartitionSendError, PartitionSendResult, ProduceOperation, Operation};
use self::partition::controller::PartitionImpl;


#[derive(Debug, PartialEq)]
pub struct EventStreamOptions {
    pub name: String,
    pub num_partitions: u16,
    pub event_retention: Duration,
    pub max_segment_duration: Duration,
    pub segment_max_size_bytes: usize,
}


impl Default for EventStreamOptions {
    fn default() -> Self {
        EventStreamOptions {
            name: "default".to_owned(),
            num_partitions: 1,
            event_retention: Duration::max_value(),     // For-ev-er
            max_segment_duration: Duration::days(1),    // 24 hours
            segment_max_size_bytes: 1024 * 1024 * 1024, // 1GB
        }
    }
}




pub fn init_existing_event_stream(event_stream_storage_dir: PathBuf, options: EventStreamOptions, loop_handles: &mut LoopHandles) -> Result<EventStreamRef, io::Error> {
    use self::partition::{initialize_existing_partition, get_partition_data_dir, run_partition};

    debug!("Starting initialization of existing event stream with: {:?}", &options);
    let partition_numbers = determine_existing_partition_dirs(&event_stream_storage_dir)?;
    debug!("Initializing {} partition(s)", partition_numbers.len());

    let mut partition_refs = Vec::with_capacity(partition_numbers.len());
    for partition_num in partition_numbers {
        let partition_ref = initialize_existing_partition(partition_num, &event_stream_storage_dir, &options)?;
        partition_refs.push(partition_ref);
    }

    partition_refs.sort_by_key(|part| part.partition_num());

    Ok(EventStreamRef {
        name: options.name,
        partitions: partition_refs,
    })
}

pub fn init_new_event_stream(event_stream_storage_dir: PathBuf, options: EventStreamOptions, loop_handles: &mut LoopHandles) -> Result<EventStreamRef, io::Error> {
    use self::partition::initialize_new_partition;

    debug!("Starting initialization of new event stream with: {:?}", &options);
    let partition_count = options.num_partitions;
    ::std::fs::create_dir_all(&event_stream_storage_dir)?;

    let mut partition_refs: Vec<PartitionRef> = Vec::with_capacity(partition_count as usize);

    for i in 0..partition_count {
        let partition_num: ActorId = i + 1;
        let partition_ref = initialize_new_partition(partition_num, &event_stream_storage_dir, &options)?;

        // We're appending these in order so that they can be indexed up by partition number later
        partition_refs.push(partition_ref);
    }

    let EventStreamOptions{name, ..} = options;
    debug!("Finished initializing {} partitions for event stream: '{}'", partition_count, &name);

    Ok(EventStreamRef{
        name: name,
        partitions: partition_refs,
    })
}


pub fn get_event_steam_data_dir(server_storage_dir: &Path, event_stream_name: &str) -> PathBuf {
    server_storage_dir.join(event_stream_name)
}

//TODO: Just save a file that contains the state of all the event streams and their partition directories instead of trying to figure it out based on conventions
fn determine_existing_partition_dirs(event_stream_dir: &Path) -> io::Result<Vec<ActorId>> {
    let files = ::std::fs::read_dir(event_stream_dir)?;
    let mut partition_numbers = Vec::with_capacity(files.size_hint().0);
    for entry_result in files {
        let dir_entry = entry_result?;
        if dir_entry.file_type()?.is_dir() {
            let partition_number = dir_entry.file_name().into_string().ok().and_then(|name| {
                name.parse::<ActorId>().ok()
            });

            if let Some(partition) = partition_number {
                partition_numbers.push(partition);
            }
        }
    }
    // this sort isn't really important, but it just makes partition initialization follow a deterministic order to make debugging easier
    partition_numbers.sort();
    Ok(partition_numbers)
}


#[derive(Clone, Debug)]
pub struct EventStreamRef {
    name: String,
    partitions: Vec<PartitionRef>,
}

impl EventStreamRef {
    pub fn new(name: String, partitions: Vec<PartitionRef>) -> EventStreamRef {
        EventStreamRef {
            name: name,
            partitions: partitions,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn get_partition_count(&self) -> ActorId {
        self.partitions.len() as ActorId
    }

    pub fn get_partition(&mut self, partition: ActorId) -> Option<&mut PartitionRef> {
        self.partitions.get_mut(partition as usize)
    }
}




