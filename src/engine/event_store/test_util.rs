use engine::event_store::{StorageEngine, EventWriter, EventReader, StorageEngineOptions};
use flo_event::{FloEvent, FloEventId, OwnedFloEvent};

use std::sync::{Arc, Mutex};
use std::io;

pub struct TestEventReader {
    storage: Arc<Mutex<Vec<OwnedFloEvent>>>,
}

pub struct TestEventIter {
    storage: Arc<Mutex<Vec<OwnedFloEvent>>>,
    current_idx: usize,
    remaining: usize,
}
unsafe impl Send for TestEventIter {}

impl Iterator for TestEventIter {
    type Item = Result<OwnedFloEvent, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let TestEventIter{ref mut storage, ref mut current_idx, ref mut remaining} = *self;

        if *remaining == 0 {
            None
        } else {
            storage.lock().unwrap().get(*current_idx).map(|evt| {
                *remaining -= 1;
                *current_idx += 1;
                Ok(evt.clone())
            })
        }
    }
}

impl EventReader for TestEventReader {
    type Error = String;
    type Iter = TestEventIter;

    fn load_range(&mut self, _start_range: FloEventId, limit: usize) -> Self::Iter {
        TestEventIter {
            storage: self.storage.clone(),
            current_idx: 0, //TODO: consider supporting an actual start range for test iterator
            remaining: limit,
        }
    }

    fn get_highest_event_id(&mut self) -> FloEventId {
        FloEventId::new(1, 1)
    }
}

pub struct TestEventWriter {
    storage: Arc<Mutex<Vec<OwnedFloEvent>>>,
}

impl TestEventWriter {
    pub fn new() -> TestEventWriter {
        TestEventWriter {
            storage: Arc::new(Mutex::new(Vec::new()))
        }
    }

    pub fn assert_events_stored(&self, expected_data: &[&[u8]]) {
        let storage = self.storage.lock().unwrap();
        let act: Vec<&[u8]> = storage.iter().map(|evt| evt.data()).collect();
        assert_eq!(&act[..], expected_data);
    }
}

impl EventWriter for TestEventWriter {
    type Error = String;

    fn store<E: FloEvent>(&mut self, event: &E) -> Result<(), Self::Error> {
        let mut storage = self.storage.lock().unwrap();
        storage.push(event.to_owned());
        Ok(())
    }
}

pub struct TestStorageEngine;
impl StorageEngine for TestStorageEngine {
    type Reader = TestEventReader;
    type Writer = TestEventWriter;

    fn initialize(_options: StorageEngineOptions) -> Result<(Self::Writer, Self::Reader), io::Error> {
        let storage = Arc::new(Mutex::new(Vec::new()));
        let writer = TestEventWriter {
            storage: storage.clone()
        };
        let reader = TestEventReader {
            storage: storage
        };
        Ok((writer, reader))
    }
}
