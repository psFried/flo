mod client;
mod cache;

pub use self::client::{ClientImpl, NamespaceGlob};

use engine::api::{ConnectionId, ConsumerMessage, ClientConnect, PeerVersionMap};
use engine::version_vec::VersionVector;
use protocol::{ProtocolMessage, ErrorMessage, ErrorKind};
use event::{FloEvent, OwnedFloEvent, FloEventId};
use std::sync::{Arc, mpsc};
use std::thread;
use std::collections::HashMap;

use self::cache::Cache;
use server::MemoryLimit;
use engine::event_store::EventReader;

struct ManagerState<R: EventReader + 'static> {
    event_reader: R,
    my_sender: mpsc::Sender<ConsumerMessage>,
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

pub struct ConsumerManager<R: EventReader + 'static> {
    consumers: ConsumerMap,
    state: ManagerState<R>,
}

impl <R: EventReader + 'static> ConsumerManager<R> {
    pub fn new(reader: R, sender: mpsc::Sender<ConsumerMessage>, greatest_event_id: FloEventId, max_cached_events: usize, max_cache_memory: MemoryLimit) -> Self {
        ConsumerManager {
            consumers: ConsumerMap::new(),
            state: ManagerState {
                my_sender: sender,
                event_reader: reader,
                greatest_event_id: greatest_event_id,
                cache: Cache::new(max_cached_events, max_cache_memory, greatest_event_id),
            },
        }
    }

    /// Process a message, which involves sending out any responses directly over client channels
    /// Returning an error from this method is considered a fatal error, and the consumer manager will be shutdown
    pub fn process(&mut self, message: ConsumerMessage) -> Result<(), String> {
        trace!("Got message: {:?}", message);
        match message {
            ConsumerMessage::ClientConnect(connect) => {
                self.consumers.add(connect);
                Ok(())
            }
            ConsumerMessage::StartConsuming(connection_id, namespace, limit) => {
                self.require_client(connection_id, move |mut client, mut state| {
                    ConsumerManager::start_consuming(state, client, namespace, limit)
                })
            }
            ConsumerMessage::ContinueConsuming(connection_id, event_id, limit) => {
                let result = self.require_client(connection_id, move |mut client, mut state| {
                    //TODO: remove limit from continueConsuming message, since the reader thread has no idea which ones actually match the namespace
                    if let Some(remaining) = client.continue_consuming() {
                        ConsumerManager::send_events(state, client, event_id, remaining);
                    } else {
                        warn!("Got ContinueConsuming message for connection_id: {} but client has since be moved to NotConsuming state", connection_id);
                    }
                    Ok(())
                });
                // This condition is a little weird, but not necessarily an error from the server's perspective
                if let Err(_) = result {
                    warn!("connection_id: {} was disconnected, so ContinueConsuming will not be processed", connection_id);
                }
                Ok(())
            }
            ConsumerMessage::EventLoaded(connection_id, event) => {
                //TODO: think about caching events as they are loaded from disk. Currently we prefer to only cache the most recent events
                self.state.update_greatest_event(event.id);
                self.consumers.send_event(connection_id, Arc::new(event))
            }
            ConsumerMessage::EventPersisted(_connection_id, event) => {
                self.state.update_greatest_event(event.id);
                let event_rc = self.state.cache.insert(event);
                self.consumers.send_event_to_all(event_rc)
            }
            ConsumerMessage::UpdateMarker(connection_id, event_id) => {
                self.consumers.update_consumer_position(connection_id, event_id)
            }
            ConsumerMessage::Disconnect(connection_id) => {
                debug!("Removing client: {}", connection_id);
                self.consumers.remove(connection_id);
                Ok(())
            }
            ConsumerMessage::StartPeerReplication(peer_version_info) => {
                self.start_peer_replication(peer_version_info)
            }
            m @ _ => {
                error!("Got unhandled message: {:?}", m);
                panic!("Got unhandled message: {:?}", m);
            }
        }
    }

    fn start_peer_replication(&mut self, peer_version_info: PeerVersionMap) -> Result<(), String> {
        let PeerVersionMap{connection_id, from_actor, actor_versions} = peer_version_info;

        debug!("Attempting to start peer replication to connection_id: {}, actor_id: {}", connection_id, from_actor);
        self.require_client(connection_id, |client, manager_state|{
            VersionVector::from_vec(actor_versions).map_err(|err| {
                ErrorMessage {
                    op_id: 0,
                    kind: ErrorKind::InvalidVersionVector,
                    description: err,
                }
            }).and_then(|version_vec| {
                let start_id = version_vec.min();
                client.start_peer_replication(version_vec).map_err(|err| {
                    ErrorMessage {
                        op_id: 0,
                        kind: ErrorKind::InvalidConsumerState,
                        description: err,
                    }
                }).map(|()| start_id)
            }).map(|start_id| {
                ConsumerManager::send_events(manager_state, client, start_id, ::std::u64::MAX);
            })
        })
    }


    fn require_client<F>(&mut self, connection_id: ConnectionId, with_client: F) -> Result<(), String>
            where F: FnOnce(&mut ClientImpl, &mut ManagerState<R>) -> Result<(), ErrorMessage> {

        let ConsumerManager{ref mut consumers, ref mut state, ..} = *self;

        consumers.get_mut(connection_id).and_then(|client| {

            if let Err(message) = with_client(client, state) {
                debug!("Sending error message to connection_id: {} - {:?}", connection_id, message);
                client.send_message(ProtocolMessage::Error(message))
            } else {
                Ok(())
            }
        })
    }

    fn start_consuming(state: &mut ManagerState<R>, client: &mut ClientImpl, namespace: String, limit: u64) -> Result<(), ErrorMessage> {

        let namespace_glob = NamespaceGlob::new(&namespace).map_err(|description| {
            ErrorMessage{
                op_id: 0,
                kind: ErrorKind::InvalidNamespaceGlob,
                description: description
            }
        })?; // early return if namespace glob parse fails

        let start_id = {
            let version_vec = client.consume_from_namespace(namespace_glob, limit).map_err(|description| {
                ErrorMessage{
                    op_id: 0,
                    kind: ErrorKind::InvalidConsumerState,
                    description: description
                }
            })?; // early return if the client is not in a valid state to start consuming
            version_vec.min()
        };

        ConsumerManager::send_events(state, client, start_id, limit);
        Ok(())
    }

    fn send_events(state: &mut ManagerState<R>, client: &mut ClientImpl, start_id: FloEventId, limit: u64) {
        let last_cache_evicted = state.cache.last_evicted_id();
        let connection_id = client.connection_id;
        debug!("connection_id: {} starting to consume starting at: {:?}, cache last evicted id: {:?}", connection_id, start_id, last_cache_evicted);
        if start_id < last_cache_evicted {

            consume_from_file(state.my_sender.clone(), connection_id, &mut state.event_reader, start_id, limit);

        } else {
            debug!("Sending events from cache for connection_id: {}", connection_id);
            let mut remaining = limit;
            state.cache.do_with_range(start_id, |id, event| {
                if client.should_send_event(&*event) {
                    trace!("Sending event from cache. connection_id: {}, event_id: {:?}", connection_id, id);
                    remaining -= 1;
                    let result = client.send_event(event.clone());
                    result.is_ok() && remaining > 0
                } else {
                    trace!("Not sending event: {:?} to connection_id: {} due to mismatched namespace", id, connection_id);
                    true
                }
            });

            if remaining > 0 {
                debug!("Sent all existing events for connection_id: {}, Awaiting new Events", connection_id);
                client.send_message_log_error(ProtocolMessage::AwaitingEvents, "AwaitingEvents");
            } else {
                debug!("Finished sending requested number of events for connection_id: {}, not awaiting more", connection_id);
            }
        }
    }
}

fn consume_from_file<R: EventReader + 'static>(event_sender: mpsc::Sender<ConsumerMessage>,
                                               connection_id: ConnectionId,
                                               event_reader: &mut R,
                                               start_id: FloEventId,
                                               limit: u64) {

    // need to read event from disk since it isn't in the cache
    let event_iter = event_reader.load_range(start_id, limit as usize);

    thread::spawn(move || {
        let mut sent_events = 0;
        let mut last_sent_id = FloEventId::zero();
        for event in event_iter {
            match event {
                Ok(owned_event) => {
                    trace!("Reader thread sending event: {:?} to consumer manager", owned_event.id());
                    //TODO: is unwrap the right thing here?
                    last_sent_id = *owned_event.id();
                    event_sender.send(ConsumerMessage::EventLoaded(connection_id, owned_event)).expect("Failed to send EventLoaded message");
                    sent_events += 1;
                }
                Err(err) => {
                    error!("Error reading event: {:?}", err);
                    //TODO: send error message to consumer manager instead of just dying silently
                    break;
                }
            }
        }
        debug!("Finished reader thread for connection_id: {}, sent_events: {}, last_send_event: {:?}", connection_id, sent_events, last_sent_id);
        if sent_events < limit as usize {
            let continue_message = ConsumerMessage::ContinueConsuming(connection_id, last_sent_id, limit - sent_events as u64);
            event_sender.send(continue_message).expect("Failed to send continue_message");
        }
    });
}

pub struct ConsumerMap(HashMap<ConnectionId, ClientImpl>);
impl ConsumerMap {
    pub fn new() -> ConsumerMap {
        ConsumerMap(HashMap::with_capacity(32))
    }

    pub fn add(&mut self, connect: ClientConnect) {
        let connection_id = connect.connection_id;
        self.0.insert(connection_id, ClientImpl::from_client_connect(connect));
    }

    pub fn remove(&mut self, connection_id: ConnectionId) {
        self.0.remove(&connection_id);
    }

    pub fn get_mut(&mut self, connection_id: ConnectionId) -> Result<&mut ClientImpl, String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("No Client exists for connection id: {}", connection_id)
        })
    }

    pub fn update_consumer_position(&mut self, connection_id: ConnectionId, new_position: FloEventId) -> Result<(), String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("Consumer: {} does not exist. Cannot update position", connection_id)
        }).map(|consumer| {
            consumer.update_version_vector(new_position);
        })
    }

    pub fn send_event(&mut self, connection_id: ConnectionId, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("Cannot send event to consumer because consumer: {} does not exist", connection_id)
        }).and_then(|mut client| {
            ConsumerMap::maybe_send_event_to_client(client, &event)
        })
    }

    pub fn send_event_to_all(&mut self, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        for client in self.0.values_mut() {
            let result = ConsumerMap::maybe_send_event_to_client(client, &event);
            if let Err(message) = result {
                warn!("Failed to send event: {} to connection_id: {} due to error: '{}'", event.id(), client.connection_id, message);
            }
        }
        Ok(())
    }

    fn maybe_send_event_to_client(client: &mut ClientImpl, event: &Arc<OwnedFloEvent>) -> Result<(), String> {
        let should_send = client.should_send_event(&**event);
        trace!("Checking to send event: {:?}, to connection_id: {}, {:?}", event.id(), client.connection_id(), should_send);
        if should_send {
            client.send_event(event.clone())
        } else {
            Ok(())
        }
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

    use futures::sync::mpsc::{unbounded, UnboundedReceiver};

    const SUBJECT_ACTOR_ID: ActorId = 1;

    fn subject_with_existing_events(events: Vec<OwnedFloEvent>) -> (ConsumerManager<MockEventReader>, Receiver<ConsumerMessage>) {
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
        let input = ConsumerMessage::StartPeerReplication(PeerVersionMap{
            connection_id: connection_id,
            from_actor: peer_actor_id,
            actor_versions: peer_versions,
        });

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
            timestamp: ::time::now(),
            parent_id: None,
            namespace: "/green/onions".to_owned(),
            data: vec![1, 2, 3, 4, 5],
        }
    }

    fn event_persisted(client_id: ConnectionId, event: OwnedFloEvent, subject: &mut ConsumerManager<MockEventReader>) {
        let event_id = event.id;
        let message = ConsumerMessage::EventPersisted(client_id, event);
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
        subject.process(ConsumerMessage::ClientConnect(connect)).expect("Failed to process client connect");
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

        fn get_highest_event_id(&mut self) -> FloEventId {
            self.stored_events.lock().unwrap().iter().map(|evt| evt.id).max().unwrap_or(FloEventId::new(0, 0))
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


