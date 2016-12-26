use event_store::{StorageEngine, EventReader};
use flo_event::{FloEvent, FloEventId, OwnedFloEvent};

use std::sync::{Arc, Mutex};
use std::io;
use std::path::Path;


pub struct TestEventReader {
    storage: Arc<Mutex<Vec<OwnedFloEvent>>>,
}

pub struct TestEventIter {
    storage: Arc<Mutex<Vec<OwnedFloEvent>>>,
    current_idx: usize,
    remaining: usize,
}

impl Iterator for TestEventIter {
    type Item = Result<OwnedFloEvent, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let TestEventIter{ref mut storage, ref mut current_idx, ref mut remaining} = *self;

        if *remaining == 0 {
            None
        } else {
            storage.lock().unwrap().get(*current_idx).map(|evt| {
                *remaining -= 1;
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

pub struct TestStorageEngine {
    storage: Arc<Mutex<Vec<OwnedFloEvent>>>,
}

impl TestStorageEngine {
    pub fn new() -> TestStorageEngine {
        TestStorageEngine {
            storage: Arc::new(Mutex::new(Vec::new()))
        }
    }


    pub fn assert_events_stored(&self, expected_data: &[&[u8]]) {
        let storage = self.storage.lock().unwrap();
        let act: Vec<&[u8]> = storage.iter().map(|evt| evt.data()).collect();
        assert_eq!(&act[..], expected_data);
    }
}

impl StorageEngine for TestStorageEngine {
    type Error = String;
    type Reader = TestEventReader;

    fn initialize(storage_dir: &Path, namespace: &str, max_num_events: usize) -> Result<(Self, Self::Reader), io::Error> {
        let storage = Arc::new(Mutex::new(Vec::new()));
        let writer = TestStorageEngine {
            storage: storage.clone()
        };
        let reader = TestEventReader {
            storage: storage
        };
        Ok((writer, reader))
    }

    fn store<E: FloEvent>(&mut self, event: &E) -> Result<(), Self::Error> {
        let mut storage = self.storage.lock().unwrap();
        storage.push(event.to_owned());
        Ok(())
    }
}
