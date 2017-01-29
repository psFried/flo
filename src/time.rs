use chrono::{UTC, TimeZone};

use flo_event::Timestamp;

const NANOS_IN_MILLISECOND: u64 = 1_000_000u64;
const MILLIS_IN_SECOND: u64 = 1_000;

pub fn millis_since_epoch(time: Timestamp) -> u64 {
    debug_assert!(time.timestamp() >= 0, "Timestamp must be after the unix epoch");
    let secs = time.timestamp() as u64;
    let millis = time.timestamp_subsec_nanos() as u64 / NANOS_IN_MILLISECOND;
    (secs * MILLIS_IN_SECOND) + millis
}

pub fn from_millis_since_epoch(millis_since_unix_epoch: u64) -> Timestamp {
    let seconds = millis_since_unix_epoch / MILLIS_IN_SECOND;
    let subsec_nanos = (millis_since_unix_epoch % MILLIS_IN_SECOND) * NANOS_IN_MILLISECOND;
    UTC.timestamp(seconds as i64, subsec_nanos as u32)
}

pub fn now() -> Timestamp {
    UTC::now()
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::UTC;

    #[test]
    fn timestamp_is_converted_from_u64_and_back() {
        let start = 23456;
        let as_timestamp = from_millis_since_epoch(start);
        let result = millis_since_epoch(as_timestamp);
        assert_eq!(start, result);
    }

    #[test]
    #[should_panic]
    fn millis_since_epoch_panics_if_timestamp_is_prior_to_unix_epoch() {
        let early_timestamp = UTC.ymd(1932, 1, 10).and_hms(9, 30, 05);
        let _ = millis_since_epoch(early_timestamp);
    }
}
