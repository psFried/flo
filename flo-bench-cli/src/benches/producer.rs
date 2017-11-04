
use std::time::Instant;

use tic::{
    Sample,
    Clocksource,
    Sender,
};

use ::Metric;
use super::connect;


pub struct ProducerBenchmark {
    server_addr: String,
    tic_sender: Sender<Metric>,
    clocksource: Clocksource,
    end_time: Instant,
    event_namespaces: Vec<String>,
    current_ns_index: usize,
    data_length: usize,
}

impl ProducerBenchmark {
    pub fn new(addr: String, sender: Sender<Metric>, clock: Clocksource, end_time: Instant) -> ProducerBenchmark {
        ProducerBenchmark {
            server_addr: addr,
            tic_sender: sender,
            clocksource: clock,
            end_time: end_time,
            current_ns_index: 0,
            event_namespaces: Vec::new(),
            data_length: 0,
        }
    }

    pub fn run(mut self) -> Result<(), String> {
        let mut connection = connect(self.server_addr)?;

        let data = vec![8; self.data_length];

        let mut count = 0;
        while Instant::now() < self.end_time {
            let ns = {
                if self.current_ns_index >= self.event_namespaces.len() {
                    self.current_ns_index = 0;
                }
                self.event_namespaces.get(self.current_ns_index).cloned().unwrap_or("/events".to_owned())
            };

            let data = data.clone();

            let start = self.clocksource.counter();
            let id = connection.produce(ns, data).map_err(|err| {
                format!("Error producing event: {:?}", err)
            })?;
            let end = self.clocksource.counter();
            count += 1;
            let sample = Sample::new(start, end, Metric::Produce);
            self.tic_sender.send(sample).map_err(|terr| {
                format!("Failed to send sample: {}: {:?}", count, terr)
            })?;

            if count % 500 == 0 {
                println!("Produced id: {} event count: {}", id, count);
            }
        }
        println!("Finished producing {} total events", count);
        Ok(())
    }
}
