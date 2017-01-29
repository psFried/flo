
use flo_event::FloEventId;
use std::time::Duration;



pub struct ProducerMetrics {
    slow_event_threshold: Duration,
    total_events_saved: u64,
}

impl ProducerMetrics {
    pub fn new() -> ProducerMetrics {
        ProducerMetrics::with_threshold(Duration::from_millis(2))
    }

    pub fn with_threshold(slow_event_threshold: Duration) -> ProducerMetrics {
        ProducerMetrics {
            slow_event_threshold: slow_event_threshold,
            total_events_saved: 0,
        }
    }

    pub fn event_persisted(&mut self, event_id: FloEventId, time_in_channel: Duration, storage_time: Duration) {
        self.total_events_saved += 1;

        let total_time = time_in_channel + storage_time;

        if total_time > self.slow_event_threshold {
            warn!("Slow persistence of event: {:?} - total_time: {}, storage_time: {:.6}, waiting_in_channel: {:.6}",
                    event_id, as_millis(total_time), as_millis(storage_time), as_millis(time_in_channel));
        } else {
            debug!("Persistence time: total: {:.6}, storage: {:.6}, waiting_in_channel: {:.6}",
                    as_millis(total_time), as_millis(storage_time), as_millis(time_in_channel));
        }
    }
}

const NANOS_IN_MILLISECOND: f64 = 1_000_000f64;

fn as_millis(dur: Duration) -> f64 {
    let mul = dur.as_secs() as f64;

    dur.subsec_nanos() as f64 / NANOS_IN_MILLISECOND * mul
}
