mod client;
mod cache;

pub use self::client::{Client, ClientState, ClientSendError, ConsumingState};
use self::client::NamespaceGlob;

use engine::api::{ConnectionId, ConsumerMessage, ClientConnect, PeerVersionMap};
use protocol::{ServerMessage, ProtocolMessage, ErrorMessage, ErrorKind};
use event::{FloEvent, OwnedFloEvent, FloEventId};
use std::sync::{Arc, mpsc};
use std::thread;
use std::collections::HashMap;

use self::cache::Cache;
use server::MemoryLimit;
use engine::event_store::EventReader;

pub struct ConsumerManager<R: EventReader + 'static> {
    event_reader: R,
    my_sender: mpsc::Sender<ConsumerMessage>,
    consumers: ConsumerMap,
    greatest_event_id: FloEventId,
    cache: Cache
}

impl <R: EventReader + 'static> ConsumerManager<R> {
    pub fn new(reader: R, sender: mpsc::Sender<ConsumerMessage>, greatest_event_id: FloEventId, max_cached_events: usize, max_cache_memory: MemoryLimit) -> Self {
        ConsumerManager {
            my_sender: sender,
            event_reader: reader,
            consumers: ConsumerMap::new(),
            greatest_event_id: greatest_event_id,
            cache: Cache::new(max_cached_events, max_cache_memory, greatest_event_id),
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
                self.start_consuming(connection_id, namespace, limit)
            }
            ConsumerMessage::ContinueConsuming(connection_id, event_id, namespace, limit) => {
                match self.consumers.update_consumer_position(connection_id, event_id) {
                    Ok(()) => self.start_consuming(connection_id, namespace, limit),
                    Err(_ignore) => {
                        /*
                        If we reach this block, it's because the connection was disconnected prior to the reader thread completing.
                        This probably isn't a server error, but is probably an unexpected disconnect.
                        */
                        warn!("cannot ContinueConsuming for Consumer: {} because the client has been disconnected", connection_id);
                        Ok(())
                    }
                }
            }
            ConsumerMessage::EventLoaded(connection_id, event) => {
                self.update_greatest_event(event.id);
                self.consumers.send_event(connection_id, Arc::new(event))
            }
            ConsumerMessage::EventPersisted(_connection_id, event) => {
                self.update_greatest_event(event.id);
                let event_rc = self.cache.insert(event);
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

    fn start_peer_replication(&mut self, _peer_version_info: PeerVersionMap) -> Result<(), String> {
        //TODO: send events to peer
        Err("oh shit".to_owned())
    }

    fn update_greatest_event(&mut self, id: FloEventId) {
        if id > self.greatest_event_id {
            self.greatest_event_id = id;
        }
    }

    fn start_consuming(&mut self, connection_id: ConnectionId, namespace: String, limit: u64) -> Result<(), String> {
        let ConsumerManager{ref mut consumers, ref mut event_reader, ref mut my_sender, ref cache, ..} = *self;

        consumers.get_mut(connection_id).map(|mut client| {
            let start_id = client.get_current_position();

            match NamespaceGlob::new(&namespace) {
                Ok(namespace_glob) => {
                    let last_cache_evicted = cache.last_evicted_id();
                    debug!("Client: {} starting to consume starting at: {:?}, cache last evicted id: {:?}", connection_id, start_id, last_cache_evicted);
                    if start_id < cache.last_evicted_id() {

                        consume_from_file(my_sender.clone(), client, event_reader, start_id, namespace_glob, namespace, limit);

                    } else {
                        debug!("Sending events from cache for connection: {}", connection_id);
                        client.start_consuming(ConsumingState::forward_from_memory(start_id, namespace_glob, limit as u64));

                        let mut remaining = limit;
                        cache.do_with_range(start_id, |id, event| {
                            if client.event_namespace_matches((*event).namespace()) {
                                trace!("Sending event from cache. connection_id: {}, event_id: {:?}", connection_id, id);
                                remaining -= 1;
                                client.send(ServerMessage::Event((*event).clone())).unwrap(); //TODO: something better than unwrap
                                remaining > 0
                            } else {
                                trace!("Not sending event: {:?} to client: {} due to mismatched namespace", id, connection_id);
                                true
                            }
                        });
                        debug!("Sent all existing events for connection_id: {}, Awaiting new Events", connection_id);
                        client.send(ServerMessage::Other(ProtocolMessage::AwaitingEvents)).unwrap();
                    }
                }
                Err(ns_err) => {
                    debug!("Client: {} send invalid namespace glob pattern. Sending error", connection_id);
                    let message = namespace_glob_error(ns_err);
                    client.send(message).unwrap();
                }
            }
        })
    }

}

fn namespace_glob_error(description: String) -> ServerMessage {
    let err = ErrorMessage {
        op_id: 0,
        kind: ErrorKind::InvalidNamespaceGlob,
        description: description,
    };
    ServerMessage::Other(ProtocolMessage::Error(err))
}

fn consume_from_file<R: EventReader + 'static>(event_sender: mpsc::Sender<ConsumerMessage>, client: &mut Client, event_reader: &mut R, start_id: FloEventId, namespace_glob: NamespaceGlob, namespace_glob_string: String, limit: u64) {
    let connection_id = client.connection_id;
    // need to read event from disk since it isn't in the cache
    let event_iter = event_reader.load_range(start_id, limit as usize);
    client.start_consuming(ConsumingState::forward_from_file(start_id, namespace_glob, limit));

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
            let continue_message = ConsumerMessage::ContinueConsuming(connection_id, last_sent_id, namespace_glob_string, limit - sent_events as u64);
            event_sender.send(continue_message).expect("Failed to send continue_message");
        }
        //TODO: else send ConsumerCompleted message
    });
}

pub struct ConsumerMap(HashMap<ConnectionId, Client>);
impl ConsumerMap {
    pub fn new() -> ConsumerMap {
        ConsumerMap(HashMap::with_capacity(32))
    }

    pub fn add(&mut self, connect: ClientConnect) {
        let connection_id = connect.connection_id;
        self.0.insert(connection_id, Client::from_client_connect(connect));
    }

    pub fn remove(&mut self, connection_id: ConnectionId) {
        self.0.remove(&connection_id);
    }

    pub fn get_mut(&mut self, connection_id: ConnectionId) -> Result<&mut Client, String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("No Client exists for connection id: {}", connection_id)
        })
    }

    pub fn update_consumer_position(&mut self, connection_id: ConnectionId, new_position: FloEventId) -> Result<(), String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("Consumer: {} does not exist. Cannot update position", connection_id)
        }).map(|consumer| {
            consumer.set_position(new_position);
        })
    }

    pub fn send_event(&mut self, connection_id: ConnectionId, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        self.0.get_mut(&connection_id).ok_or_else(|| {
            format!("Cannot send event to consumer because consumer: {} does not exist", connection_id)
        }).and_then(|mut client| {
            if client.event_namespace_matches(event.namespace()) {
                client.send(ServerMessage::Event(event)).map_err(|err| {
                    format!("Error sending event to server channel: {:?}", err)
                })
            } else {
                Ok(())
            }
        })
    }

    pub fn send_event_to_all(&mut self, event: Arc<OwnedFloEvent>) -> Result<(), String> {
        for client in self.0.values_mut() {
            trace!("Checking to send event: {:?}, to client: {}, {:?}", event.id(), client.connection_id(), client.is_awaiting_new_event());
            if client.is_awaiting_new_event() && client.event_namespace_matches(event.namespace()) {
                client.send(ServerMessage::Event(event.clone())).unwrap();
            }
        }
        Ok(())
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use event::*;
    use engine::api::*;
    use protocol::*;
    use engine::event_store::EventReader;
    use server::{MemoryLimit, MemoryUnit};
    use std::sync::mpsc::{channel, Receiver};

    use futures::sync::mpsc::{unbounded, UnboundedReceiver};

    const SUBJECT_ACTOR_ID: ActorId = 1;

    fn subject_with_existing_events(events: Vec<OwnedFloEvent>) -> (ConsumerManager<MockEventReader>, Receiver<ConsumerMessage>) {
        let (tx, rx) = channel();
        let mem_limit = MemoryLimit::new(10, MemoryUnit::Megabyte);
        let reader = MockEventReader::new(events);
        let consumer_manager = ConsumerManager::new(reader, tx, FloEventId::zero(), 100, mem_limit);
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

//    #[test] TODO: apparently, we're still a ways off from being able to get this test passing
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

        let peer_versions = vec![id(SUBJECT_ACTOR_ID, 1), id(3, 2)];
        let input = ConsumerMessage::StartPeerReplication(PeerVersionMap{
            connection_id: connection_id,
            from_actor: peer_actor_id,
            actor_versions: peer_versions,
        });

        subject.process(input).expect("failed to processs startPeerReplication");

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
                    panic!("expected event, got: {:?}", other);
                }
            }
        })
    }

    fn assert_client_message_sent<F: Fn(ServerMessage)>(client: &mut UnboundedReceiver<ServerMessage>, fun: F) {
        use futures::{Async, Stream};
        match client.poll().expect("failed to receive message") {
            Async::Ready(Some(message)) => fun(message),
            _ => {
                panic!("Expected to receive a message, got none")
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
        stored_events: Vec<OwnedFloEvent>
    }

    impl MockEventReader {
        pub fn new(events: Vec<OwnedFloEvent>) -> MockEventReader {
            MockEventReader {
                stored_events: events,
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
            let events = self.stored_events.iter()
                    .filter(|evt| evt.id > range_start)
                    .take(limit)
                    .cloned()
                    .collect();
            MockReaderIter {
                events: events
            }
        }

        fn get_highest_event_id(&mut self) -> FloEventId {
            self.stored_events.iter().map(|evt| evt.id).max().unwrap_or(FloEventId::new(0, 0))
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


