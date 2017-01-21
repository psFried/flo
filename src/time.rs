use std::time::{SystemTime, Duration, UNIX_EPOCH};

const NANOS_IN_MILLISECOND: u64 = 1_000_000u64;
const MILLIS_IN_SECOND: u64 = 1_000;

pub fn millis_since_epoch(time: SystemTime) -> u64 {
    let duration = time.duration_since(UNIX_EPOCH).expect("Time must be after unix epoch");
    (duration.subsec_nanos() as u64 / NANOS_IN_MILLISECOND) + (duration.as_secs() * MILLIS_IN_SECOND)
}

pub fn from_millis_since_epoch(millis_since_unix_epoch: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis_since_unix_epoch)
}
