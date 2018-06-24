use rmp_serde::decode::Error;

use protocol::{Term, FloInstanceId};
use event::{FloEvent, EventData, FloEventId, EventCounter, OwnedFloEvent, ActorId, Timestamp};
use engine::event_stream::partition::PersistentEvent;
use engine::controller::controller_messages::Peer;

#[derive(Debug, PartialEq)]
pub struct SystemEvent<E: FloEvent> {
    pub wrapped: E,
    pub deserialized_data: SystemEventData
}

impl <E: FloEvent> SystemEvent<E> {

    pub fn from_event(event: E) -> Result<SystemEvent<E>, Error> {
        let data = ::rmp_serde::decode::from_slice::<SystemEventData>(event.data())?;
        Ok(SystemEvent{
            wrapped: event,
            deserialized_data: data
        })
    }

    pub fn counter(&self) -> EventCounter {
        self.wrapped.id().event_counter
    }

    pub fn term(&self) -> Term {
        self.system_data().term
    }

    pub fn system_data(&self) -> &SystemEventData {
        &self.deserialized_data
    }
}

pub fn validate_data(event_data: &[u8], term: Term) -> ::std::io::Result<()> {
    use std::io;
    ::rmp_serde::decode::from_slice::<SystemEventData>(event_data).map_err(|serde_err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("Invalid system event body: {}", serde_err))
    }).and_then(|system_data| {
        if system_data.term == term {
            Ok(())
        } else {
            let msg = format!("Refusing to produce event with term: {} because current term of {} is greater", 
                    system_data.term, term);
            Err(io::Error::new(io::ErrorKind::InvalidData, msg))
        }
    })
}

impl Into<PersistentEvent> for SystemEvent<PersistentEvent> {
    fn into(self) -> PersistentEvent {
        self.wrapped
    }
}


impl SystemEvent<OwnedFloEvent> {
    pub fn new(id: FloEventId, parent: Option<FloEventId>, namespace: String, time: Timestamp, data: SystemEventData) -> SystemEvent<OwnedFloEvent> {
        let serialized = data.serialize();
        let event = OwnedFloEvent::new(id, parent, time, namespace, serialized);
        SystemEvent {
            wrapped: event,
            deserialized_data: data,
        }
    }
}

impl <E: FloEvent> EventData for SystemEvent<E> {
    fn event_namespace(&self) -> &str {
        self.wrapped.event_namespace()
    }

    fn event_parent_id(&self) -> Option<FloEventId> {
        self.wrapped.event_parent_id()
    }

    fn event_data(&self) -> &[u8] {
        self.wrapped.event_data()
    }

    fn get_precomputed_crc(&self) -> Option<u32> {
        self.wrapped.get_precomputed_crc()
    }
}

impl <E: FloEvent> FloEvent for SystemEvent<E> {

    fn id(&self) -> &FloEventId {
        self.wrapped.id()
    }
    fn timestamp(&self) -> Timestamp {
        self.wrapped.timestamp()
    }
    fn parent_id(&self) -> Option<FloEventId> {
        self.wrapped.parent_id()
    }
    fn namespace(&self) -> &str {
        self.wrapped.namespace()
    }
    fn data_len(&self) -> u32 {
        self.wrapped.data_len()
    }
    fn data(&self) -> &[u8] {
        self.wrapped.data()
    }
}


#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct InitialClusterMembership {
    pub peers: Vec<Peer>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct PartitionAssigned {
    pub peer_id: FloInstanceId,
    pub partition_num: ActorId,
}


#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum SystemEventKind {
    ClusterInitialized(InitialClusterMembership),
    NewClusterMemberJoining(Peer),
    AssignPartition(PartitionAssigned),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SystemEventData {
    pub term: Term,
    pub kind: SystemEventKind,
}

impl  SystemEventData {
    pub fn serialize(&self) -> Vec<u8> {
        ::rmp_serde::to_vec(self).unwrap()
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use event::time;

    #[test]
    fn from_event_returns_error_when_event_data_cannot_be_deserialized() {
        let data = vec![ 0xff, 0xff, 0xff, 0xff, 0x00, 0x00 ];
        let event = OwnedFloEvent::new(FloEventId::new(1, 2), None, time::now(), String::new(), data);
        let result = SystemEvent::from_event(event);
        assert!(result.is_err());
    }

    #[test]
    fn system_event_data_is_serialized_and_deserialized_inside_system_event() {
        use ::test_utils::addr;

        let data = SystemEventData {
            term: 33,
            kind: SystemEventKind::NewClusterMemberJoining(Peer {
                id: 555,
                address: addr("127.0.0.1:3000"),
            })
        };
        let id = FloEventId::new(3, 4);
        let parent = Some(FloEventId::new(2, 3));
        let time = time::from_millis_since_epoch(1234567);
        let namespace = "/system/foo".to_owned();
        let event = SystemEvent::new(id, parent, namespace, time, data);
        let as_owned = event.to_owned_event();
        let result = SystemEvent::from_event(as_owned).unwrap();
        assert_eq!(event, result);
    }
}

