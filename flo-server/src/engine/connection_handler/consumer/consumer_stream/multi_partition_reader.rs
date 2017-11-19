
use std::io;

use event::{FloEvent, EventCounter};
use engine::event_stream::partition::{PartitionReader, PersistentEvent};


pub struct MultiPartitionEventReader {
    readers: Vec<PartReaderInternal>,
}

impl MultiPartitionEventReader {

    pub fn new(readers: Vec<PartitionReader>) -> MultiPartitionEventReader {
        let inner = readers.into_iter().map(|reader| {
            PartReaderInternal {
                next_val: None,
                reader: reader,
            }
        }).collect();

        MultiPartitionEventReader {
            readers: inner,
        }
    }

    pub fn next_matching(&mut self) -> Option<io::Result<PersistentEvent>> {
        let mut min_val: EventCounter = EventCounter::max_value();
        let mut reader_index: usize = 0;

        for i in 0..self.readers.len() {
            let reader = &mut self.readers[i];
            reader.advance();

            let val = reader.next_val.as_ref().map(|result| {
                // if result is error, then we want to return it ASAP
                result.as_ref().map(|event| event.id().event_counter).unwrap_or(0)
            }).unwrap_or(EventCounter::max_value());

            if val < min_val {
                reader_index = i;
                min_val = val;
            }
        }

        self.readers[reader_index].next_val.take()
    }
}


struct PartReaderInternal {
    next_val: Option<io::Result<PersistentEvent>>,
    reader: PartitionReader,
}

impl PartReaderInternal {
    fn advance(&mut self) {
        if self.next_val.is_none() {
            let next = self.reader.next_matching();
            self.next_val = next;
        }
    }
}
