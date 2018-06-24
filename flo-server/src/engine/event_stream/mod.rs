pub mod partition;
mod highest_counter;

use std::path::{PathBuf, Path};
use std::io;

use tokio_core::reactor::Remote;
use futures::{Sink, Async, AsyncSink, StartSend, Poll};
use chrono::Duration;

use event::ActorId;
use self::partition::{PartitionRef, PartitionRefMut, initialize_existing_partition, initialize_new_partition};

pub use self::highest_counter::HighestCounter;

#[derive(Debug, PartialEq, Clone)]
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

impl EventStreamOptions {
    pub fn get_tick_interval(&self) -> Duration {
        self.max_segment_duration / 3
    }
}

pub fn init_existing_event_stream(event_stream_storage_dir: PathBuf, options: EventStreamOptions, remote: Remote, start_writable: bool) -> Result<EventStreamRefMut, io::Error> {

    debug!("Starting initialization of existing event stream with: {:?}", &options);
    let partition_numbers = determine_existing_partition_dirs(&event_stream_storage_dir)?;
    debug!("Initializing {} partition(s)", partition_numbers.len());

    let highest_counter = HighestCounter::zero();

    let mut partition_refs = Vec::with_capacity(partition_numbers.len());
    for partition_num in partition_numbers {
        let partition_ref = initialize_existing_partition(partition_num, &event_stream_storage_dir, &options, highest_counter.clone(), start_writable)?;
        partition_refs.push(partition_ref);
    }

    partition_refs.sort_by_key(|part| part.partition_num());

    let tick_interval = options.get_tick_interval();
    let event_stream = EventStreamRefMut {
        event_stream_options: options,
        partitions: partition_refs,
        highest_counter,
    };

    start_tick_timer(remote, event_stream.clone_ref(), tick_interval);

    Ok(event_stream)
}

pub fn init_new_event_stream(event_stream_storage_dir: PathBuf, options: EventStreamOptions, remote: Remote, start_writable: bool) -> Result<EventStreamRefMut, io::Error> {

    debug!("Starting initialization of new event stream with: {:?}", &options);
    let partition_count = options.num_partitions;
    ::std::fs::create_dir_all(&event_stream_storage_dir)?;

    let mut partition_refs: Vec<PartitionRefMut> = Vec::with_capacity(partition_count as usize);
    let highest_counter = HighestCounter::zero();
    for i in 0..partition_count {
        let partition_num: ActorId = i + 1;
        let partition_ref = initialize_new_partition(partition_num, &event_stream_storage_dir, &options, highest_counter.clone(), start_writable)?;

        // We're appending these in order so that they can be indexed up by partition number later
        partition_refs.push(partition_ref);
    }

    let tick_interval = options.get_tick_interval();
    debug!("Finished initializing {} partitions for event stream: '{}'", partition_count, options.name);
    let event_stream = EventStreamRefMut {
        event_stream_options: options,
        partitions: partition_refs,
        highest_counter: highest_counter,
    };
    start_tick_timer(remote, event_stream.clone_ref(), tick_interval);
    Ok(event_stream)
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

#[derive(Debug)]
pub struct EventStreamRefMut {
    partitions: Vec<PartitionRefMut>,
    event_stream_options: EventStreamOptions,
    highest_counter: HighestCounter,
}

impl EventStreamRefMut {
    pub fn get_name(&self) -> &str {
        &self.event_stream_options.name
    }

    pub fn clone_ref(&self) -> EventStreamRef {
        let name = self.get_name().to_owned();
        let partitions = self.partitions.iter().map(|part| part.clone_ref()).collect::<Vec<PartitionRef>>();

        EventStreamRef {
            name,
            partitions
        }
    }

    pub fn set_writable_partition(&mut self, partition_num: ActorId) {
        info!("Setting partition: {} for event_stream: '{}' to writable", partition_num, self.get_name());
        for partition in self.partitions.iter_mut() {
            if partition.partition_num() == partition_num {
                partition.set_writable();
            } else {
                partition.set_read_only();
            }
        }
    }

    pub fn add_new_partition(&mut self, partition_num: ActorId, event_stream_data: &Path) -> io::Result<()> {
        info!("Adding new partition: {} to event_stream: '{}'", partition_num, self.get_name());
        if self.partitions.iter().any(|p| p.partition_num() == partition_num) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Partition number already exists"));
        }
        if !self.partition_number_is_next_sequential(partition_num) {
            return Err(io::Error::new(io::ErrorKind::Other, "Partition number is not the next sequential number"));
        }

        let highest_counter = self.highest_counter.clone();
        let partition = initialize_new_partition(partition_num, event_stream_data, &self.event_stream_options, highest_counter, false)?;
        self.partitions.push(partition);
        Ok(())
    }

    fn partition_number_is_next_sequential(&self, partition_num: ActorId) -> bool {
        self.partitions.len() == partition_num as usize + 1
    }
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

    pub fn partitions(&self) -> &[PartitionRef] {
        &self.partitions
    }

    pub fn get_partition(&mut self, partition: ActorId) -> Option<&mut PartitionRef> {
        self.partitions.get_mut(partition as usize - 1)
    }
}


fn start_tick_timer(remote: Remote, event_stream: EventStreamRef, tick_interval: Duration) {
    use tokio_core::reactor::Interval;
    use futures::{Stream, Future};

    remote.spawn(move |handle| {
        let interval = Interval::new(tick_interval.to_std().unwrap(), handle).expect("Failed to create timer interval");
        interval.map_err(|io_err| {
            error!("Failed to fire timer: {:?}", io_err);
        }).forward(TickTimerSink(event_stream))
            .map(|_| ())
    });
}

pub struct TickTimerSink(EventStreamRef);

impl Sink for TickTimerSink {
    type SinkItem = ();
    type SinkError = ();

    fn start_send(&mut self, _item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        for partition in self.0.partitions.iter_mut() {
            partition.tick().map_err(|io_err| {
                error!("Failed to send Tick operation to Partition: {}, {:?}", partition.partition_num(), io_err);
            })?;
        }
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}



