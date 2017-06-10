mod client;
mod cache;
mod filecursor;

use engine::api::{ConnectionId, ClientConnect, ConsumerManagerMessage, ReceivedMessage, NamespaceGlob, ConsumerState};
use protocol::{ProtocolMessage, ErrorMessage, ErrorKind, ConsumerStart, ServerMessage};
use event::{FloEvent, OwnedFloEvent, FloEventId, ActorId, VersionVector};
use std::sync::{Arc, mpsc};
use std::thread;
use std::collections::HashMap;

use self::client::{ClientConnection, ConnectionContext, CursorType};
use self::cache::Cache;
use server::MemoryLimit;
use engine::event_store::EventReader;
use futures::sync::mpsc::UnboundedSender;
use channels::Sender;

pub const DEFAULT_BATCH_SIZE: u64 = 10_000;

struct ManagerState<R: EventReader + 'static> {
    event_reader: R,
    my_sender: mpsc::Sender<ConsumerManagerMessage>,
    greatest_event_id: FloEventId,
    cache: Cache,
}

impl <R: EventReader + 'static> ManagerState<R> {

    fn update_greatest_event(&mut self, id: FloEventId) {
        if id > self.greatest_event_id {
            self.greatest_event_id = id;
        }
    }

}

impl <R: EventReader + 'static> ConnectionContext for ManagerState<R> {
    fn start_consuming<S: Sender<ServerMessage> + 'static>(&mut self, mut consumer_state: ConsumerState, client_sender: &S) -> Result<CursorType, String> {
        use std::clone::Clone;

        let last_cache_evicted = self.cache.last_evicted_id();
        let connection_id = consumer_state.connection_id;
        let start_id = consumer_state.version_vector.min();
        debug!("connection_id: {} starting to consume starting at: {:?}, cache last evicted id: {:?}", connection_id, start_id, last_cache_evicted);
        if start_id < last_cache_evicted {
            // Clone the client sender since we'll need to pass it over to the cursor's thread
            self::filecursor::start(consumer_state, (*client_sender).clone(), &mut self.event_reader).map(|cursor| {
                CursorType::File(Box::new(cursor))
            }).map_err(|io_err| {
                error!("Failed to create file cursor for consumer: {:?}, err: {:?}", connection_id, io_err);
                format!("Error creating cursor: {}", io_err)
            })
        } else {
            debug!("Sending events from cache for connection_id: {}", connection_id);

            for (ref id, ref event) in self.cache.iter(start_id) {
                if consumer_state.should_send_event(&*event) {
                    trace!("Sending event from cache. connection_id: {}, event_id: {:?}", connection_id, id);
                    client_sender.send(ServerMessage::Event((*event).clone())).map_err(|_| {
                        error!("Failed to send event: {} to client sender for connection_id: {}", id, connection_id);
                        format!("Failed to send event: {} to client sender for connection_id: {}", id, connection_id)
                    })?; //early return with error if send fails
                    consumer_state.event_sent(**id); // update consumer state
                } else {
                    trace!("Not sending event: {:?} to connection_id: {} due to mismatched namespace", id, connection_id);
                }
                // if the batch is not yet exhausted, then keep going
                if consumer_state.is_batch_exhausted() {
                    break;
                }
            }

            debug!("Finished sending events from cache for consumer: {:?}", consumer_state);
            Ok(CursorType::InMemory(consumer_state))
        }
    }
} 

pub struct ConsumerManager<R: EventReader + 'static> {
    consumers: ConsumerMap<UnboundedSender<ServerMessage>>,
    state: ManagerState<R>,
    default_batch_size: u64,
}

impl <R: EventReader + 'static> ConsumerManager<R> {
    pub fn new(reader: R, sender: mpsc::Sender<ConsumerManagerMessage>, greatest_event_id: FloEventId, max_cached_events: usize, max_cache_memory: MemoryLimit) -> Self {
        ConsumerManager {
            consumers: ConsumerMap::new(),
            state: ManagerState {
                my_sender: sender,
                event_reader: reader,
                greatest_event_id: greatest_event_id,
                cache: Cache::new(max_cached_events, max_cache_memory, greatest_event_id),
            },
            default_batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn process(&mut self, message: ConsumerManagerMessage) -> Result<(), String> {
        match message {
            ConsumerManagerMessage::Connect(connect) => {
                let ClientConnect {connection_id, client_addr, message_sender} = connect;

                let consumer = ClientConnection::new(connection_id, client_addr, message_sender, self.default_batch_size);
                self.consumers.add(consumer);
                Ok(())
            }
            ConsumerManagerMessage::Disconnect(connection_id, client_address) => {
                debug!("Removing client: {} at address: {}", connection_id, client_address);
                self.consumers.remove(connection_id);
                Ok(())
            }
            // TODO: Remove these three messages
            ConsumerManagerMessage::ContinueConsuming(consumer_state) => {
                unimplemented!()
            }
            ConsumerManagerMessage::StartPeerReplication(connection_id, actor_id, peer_versions) => {
                unimplemented!()
            }
            ConsumerManagerMessage::EventLoaded(connection_id, event) => {
                unimplemented!()
            }
            ConsumerManagerMessage::EventPersisted(_connection_id, event) => {
                self.state.update_greatest_event(event.id);
                let event_rc = self.state.cache.insert(event);
                self.consumers.send_event_to_all(event_rc)
            }
            ConsumerManagerMessage::Receive(received_message) => {
                self.process_received_message(received_message)
            }
        }
    }

    fn process_received_message(&mut self, ReceivedMessage{sender, message, ..}: ReceivedMessage) -> Result<(), String> {
        let ConsumerManager{ref mut consumers, ref mut state, ..} = *self;
        consumers.get_mut(sender).and_then(|client| {
            client.message_received(message, state).map_err(|()| {
                format!("Error processing message for connection_id: {}", client.connection_id)
            })
        })
    }



}

pub struct ConsumerMap<S: Sender<ServerMessage> + 'static>(HashMap<ConnectionId, ClientConnection<S>>);


impl <S: Sender<ServerMessage> + 'static> ConsumerMap<S> {
    pub fn new() -> ConsumerMap<S> {
        ConsumerMap(HashMap::with_capacity(32))
    }

    pub fn add(&mut self, conn: ClientConnection<S>) {
        let connection_id = conn.connection_id;
        self.0.insert(connection_id, conn);
    }

    pub fn remove(&mut self, connection_id: ConnectionId) {
        self.0.remove(&connection_id);
    }

    pub fn get_mut(&mut self, connection_id: ConnectionId) -> Result<&mut ClientConnection<S>, String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("No Client exists for connection id: {}", connection_id)
        })
    }

    pub fn send_event_to_all(&mut self, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        for client in self.0.values_mut() {
            let result = client.maybe_send_event(&event);
            if let Err(()) = result {
                warn!("Failed to send event: {} to connection_id: {}", event.id(), client.connection_id);
            }
        }
        Ok(())
    }
}


#[allow(dead_code, unused_variables)]
#[cfg(test)]
mod test {
    use super::*;
    use event::*;
    use engine::api::*;
    use protocol::*;
    use engine::event_store::EventReader;
    use engine::consumer::cache::Cache;
    use server::{MemoryLimit, MemoryUnit};
    use std::sync::mpsc::{channel, Receiver};
    use std::sync::{Arc, Mutex};
    use channels::MockSender;

    use futures::sync::mpsc::{unbounded, UnboundedReceiver};

    const SUBJECT_ACTOR_ID: ActorId = 1;

    fn subject_with_existing_events(events: Vec<OwnedFloEvent>) -> (ConsumerManager<MockEventReader>, Receiver<ConsumerManagerMessage>) {
        let (tx, rx) = channel();
        let max_event_id = events.iter().map(|e| *e.id()).max().unwrap_or(FloEventId::zero());
        let mem_limit = MemoryLimit::new(10, MemoryUnit::Megabyte);
        let mut cache = Cache::new(100, mem_limit, FloEventId::zero());
        for event in events.iter() {
            cache.insert(event.clone());
        }
        let reader = MockEventReader::new(events);
        let consumer_manager = ConsumerManager {
            consumers: ConsumerMap::new(),
            state: ManagerState {
                event_reader: reader,
                my_sender: tx,
                greatest_event_id: max_event_id,
                cache: cache,
            },
            default_batch_size: super::DEFAULT_BATCH_SIZE,
        };
        (consumer_manager, rx)
    }

    macro_rules! version_vec {
        ($([$actor:expr,$counter:expr]),*) => {
            {
                let mut vv = VersionVector::new();
                $( vv.update(FloEventId::new($actor, $counter)).unwrap(); )*
                vv
            }
        }
    }

    #[test]
    fn client_is_sent_all_events_greater_than_those_in_version_map_after_upgrading_to_peer() {
        let peer_actor_id = 2;
        let existing_events = vec![
            event(SUBJECT_ACTOR_ID, 1),
            event(peer_actor_id, 2),
            event(SUBJECT_ACTOR_ID, 2),
            event(3, 2),
            event(peer_actor_id, 3),
            event(3, 3)
        ];
        let (mut subject, receiver) = subject_with_existing_events(existing_events);

        let connection_id = 7;
        let mut client_receiver = client_connect(&mut subject, connection_id);

        let peer_versions = vec![id(SUBJECT_ACTOR_ID, 1), id(peer_actor_id, 3)];
        let input = ConsumerManagerMessage::StartPeerReplication(connection_id, peer_actor_id, peer_versions);

        subject.process(input).expect("failed to process startPeerReplication");

        assert_event_sent(&mut client_receiver, id(SUBJECT_ACTOR_ID, 2));
        assert_event_sent(&mut client_receiver, id(3, 2));
        assert_event_sent(&mut client_receiver, id(3, 3));
    }

    fn assert_event_sent(client: &mut UnboundedReceiver<ServerMessage>, expected_id: FloEventId) {
        assert_client_message_sent(client, |msg| {
            match msg {
                ServerMessage::Event(event_arc) => {
                    assert_eq!(expected_id, event_arc.id);
                }
                other @ _ => {
                    panic!("expected event with id: {}, got: {:?}", expected_id, other);
                }
            }
        })
    }

    fn assert_client_message_sent<F: Fn(ServerMessage)>(client: &mut UnboundedReceiver<ServerMessage>, fun: F) {
        use futures::{Async, Stream};
        use std::time::{Duration, Instant};
        let timeout = Duration::from_millis(500);
        let start_time = Instant::now();
        loop {
            match client.poll().expect("failed to receive message") {
                Async::Ready(Some(message)) => {
                    fun(message);
                    break
                },
                _ => {
                    let duration = Instant::now() - start_time;
                    if duration >= timeout {
                        panic!("Expected to receive a message, got none")
                    } else {
                        ::std::thread::sleep(Duration::from_millis(75));
                        println!("retrying polling of client channel");
                    }
                }
            }
        }
    }

    fn id(actor: ActorId, counter: EventCounter) -> FloEventId {
        FloEventId::new(actor, counter)
    }

    fn event(actor: ActorId, counter: EventCounter) -> OwnedFloEvent {
        OwnedFloEvent {
            id: FloEventId::new(actor, counter),
            timestamp: ::event::time::now(),
            parent_id: None,
            namespace: "/green/onions".to_owned(),
            data: vec![1, 2, 3, 4, 5],
        }
    }

    fn event_persisted(client_id: ConnectionId, event: OwnedFloEvent, subject: &mut ConsumerManager<MockEventReader>) {
        let event_id = event.id;
        let message = ConsumerManagerMessage::EventPersisted(client_id, event);
        subject.process(message).expect("failed to process event persisted");
    }


    fn client_connect(subject: &mut ConsumerManager<MockEventReader>, client_id: ConnectionId) -> UnboundedReceiver<ServerMessage> {
        use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};

        let (tx, rx) = unbounded();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 3000));
        let connect = ClientConnect {
            connection_id: client_id,
            client_addr: addr,
            message_sender: tx,
        };
        subject.process(ConsumerManagerMessage::Connect(connect)).expect("Failed to process client connect");
        rx
    }

    struct MockEventReader {
        stored_events: Arc<Mutex<Vec<OwnedFloEvent>>>
    }

    impl MockEventReader {
        pub fn new(events: Vec<OwnedFloEvent>) -> MockEventReader {
            MockEventReader {
                stored_events: Arc::new(Mutex::new(events)),
            }
        }

        pub fn empty() -> MockEventReader {
            MockEventReader::new(Vec::new())
        }
    }

    impl EventReader for MockEventReader {
        type Iter = MockReaderIter;
        type Error = String;
        fn load_range(&mut self, range_start: FloEventId, limit: usize) -> Self::Iter {
            let events = self.stored_events.lock().unwrap().iter()
                    .filter(|evt| evt.id > range_start)
                    .take(limit)
                    .cloned()
                    .collect();
            MockReaderIter {
                events: events
            }
        }
    }

    struct MockReaderIter {
        events: Vec<OwnedFloEvent>,
    }

    impl Iterator for MockReaderIter {
        type Item = Result<OwnedFloEvent, String>;
        fn next(&mut self) -> Option<Self::Item> {
            self.events.pop().map(|e| Ok(e))
        }
    }

}


