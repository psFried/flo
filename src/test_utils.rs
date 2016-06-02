
use server::consumer::ConsumerNotifier;
use server::namespace::Namespace;
use server::context::FloContext;
use event_store::{EventStore, PersistenceResult};
use event::{EventId, Event};
use httparse;
use std::collections::HashMap;
use std::io;
use std::path::{PathBuf, Path};


pub struct MockConsumerNotifier {
    pub notify_invokations: u32
}

impl MockConsumerNotifier {

    pub fn new() -> MockConsumerNotifier {
        MockConsumerNotifier {
            notify_invokations: 0
        }
    }

    pub fn assert_notify_was_called(&self) {
        assert!(self.notify_invokations == 1, "Expected one invocation of notify(), got: {}", self.notify_invokations);
    }
}

impl ConsumerNotifier for MockConsumerNotifier {
    fn notify(&mut self) {
        self.notify_invokations += 1;
    }
}

pub struct MockEventStore {
    events: Vec<Event>,
    get_event_greater_than_stub: HashMap<EventId, Event>,
}

impl MockEventStore {
    pub fn new() -> MockEventStore {
        MockEventStore {
            events: Vec::new(),
            get_event_greater_than_stub: HashMap::new(),
        }
    }

    pub fn stub_get_event_greater_than(&mut self, event_id: EventId, return_val: Event) {
        self.get_event_greater_than_stub.insert(event_id, return_val);
    }

    pub fn assert_event_was_stored(&self, expected_event: &Event) {
        assert!(self.events.contains(expected_event));
    }
}

impl EventStore for MockEventStore {
	fn create(_base_dir: &Path, _namespace: &str) -> Result<Self, io::Error> {
	    Ok(MockEventStore::new())
	}

    fn store(&mut self, event: Event) -> PersistenceResult {
        let event_id = event.get_id();
        self.events.push(event);
        Ok(event_id)
    }

    fn get_event_greater_than(&mut self, event_id: EventId) -> Option<&mut Event> {
        self.get_event_greater_than_stub.get_mut(&event_id)
    }
}

pub fn create_test_flo_context() -> FloContext<MockConsumerNotifier, MockEventStore> {
    FloContext::new(PathBuf::from("."))
}

pub fn create_test_namespace() -> Namespace<MockEventStore, MockConsumerNotifier> {
    let path = PathBuf::from(".");
    Namespace::new(&path, "anyOldName".to_string()).unwrap()
}


pub fn assert_response_body(expected: &str, buffer: &[u8]) {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut response = httparse::Response::new(&mut headers);
    let parse_result = response.parse(buffer).unwrap();
    assert!(parse_result.is_complete());

    let buffer_position: usize = parse_result.unwrap();
    let (_, body) = buffer.split_at(buffer_position);
    let str_body = String::from_utf8_lossy(body);
    assert_eq!(expected, str_body.trim());
}

pub fn assert_http_status(expected: u16, buffer: &[u8]) {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut response = httparse::Response::new(&mut headers);
    let parse_result = response.parse(buffer).unwrap();
    assert!(parse_result.is_complete());
    let actual_status = response.code.expect("Expected a response status code");
    assert_eq!(expected, actual_status);
}
