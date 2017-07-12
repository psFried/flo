

use std::io::{self, Seek, SeekFrom, Read, BufRead, BufReader};
use std::path::Path;
use std::fs::{OpenOptions, File};

use byteorder::{ByteOrder, BigEndian};

use engine::event_store::fs::FLO_EVT;
use event::{FloEventId, ActorId, OwnedFloEvent, Timestamp};

enum EventIterInner {
    Empty,
    NonEmpty{
        file_reader: BufReader<File>,
        max_id: FloEventId,
    },
}




pub struct FSEventIter(EventIterInner, ActorId);

impl FSEventIter {
    /// Creates a new `FSEventIter`
    ///
    /// # Arguments
    ///
    /// * `offset` - the byte offset within the file to start at. The iterator will seek this number of bytes from the start of the file
    /// * `max_id` - Represents the inclusive stopping point for this iterator. This iterator will only return events with an id <= `max_id`. This parameter should never be greater than the largest id that is know to be stored to the file. This prevents the iterator from trying to read a partially flushed event and returning an error
    /// * `path` - the path of the file to open for reading events
    /// * `actor_id` - the id of the actor that this file path corresponds to. This is provided for informational purposes only
    pub fn initialize(offset: u64, max_id: FloEventId, path: &Path, actor_id: ActorId) -> Result<FSEventIter, io::Error> {
        trace!("opening file at path: {:?}", path);
        OpenOptions::new().read(true).open(path).and_then(|mut file| {
            file.seek(SeekFrom::Start(offset)).map(|_| {
                let inner = EventIterInner::NonEmpty {
                    file_reader: BufReader::new(file),
                    max_id: max_id,
                };
                FSEventIter(inner, actor_id)
            })
        })
    }

}

impl Iterator for FSEventIter {
    type Item = Result<OwnedFloEvent, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut exhausted = false;
        let next_result = match self.0 {
            EventIterInner::Empty => None,
            EventIterInner::NonEmpty {ref mut file_reader, ref mut max_id} => {
                match has_next_event(file_reader) {
                    Ok(true) => {
                        match read_event(file_reader) {
                            Ok(event) => {
                                if event.id <= *max_id {
                                    // if this is the last event we're going to send, then change state to Empty now
                                    // this lets us avoid the attempt to read beyond the known max
                                    exhausted = event.id == *max_id;
                                    Some(Ok(event))
                                } else {
                                    None
                                }
                            }
                            Err(err) => {
                                Some(Err(err))
                            }
                        }
                    }
                    Ok(false) => {
                        None
                    }
                    Err(err) => {
                        Some(Err(err))
                    }
                }
            }
        };

        if exhausted {
            self.0 = EventIterInner::Empty;
        }

        next_result
    }
}

fn has_next_event<R: BufRead>(reader: &mut R) -> Result<bool, io::Error> {
    reader.fill_buf().map(|buffer| {
        buffer.len() >= 8 && &buffer[..8] == FLO_EVT.as_bytes()
    })
}

pub struct EventHeader {
    pub total_size: u32,
    pub event_counter: u64,
    pub actor_id: u16,
    pub parent_counter: u64,
    pub parent_actor_id: u16,
    pub timestamp: u64,
    pub namespace_length: u32,
}

impl EventHeader {

    pub fn compute_data_length(&self) -> usize {
        /*
        -4 bytes for the total size field
        -10 for event id
        -10 for parent event id
        -8 for timestamp
        -4 for namespace length field
        - the namespace itself
        -4 for the data length field
        */
        (self.total_size - 4 - 10 - 10 - 8 - 4 - self.namespace_length - 4) as usize
    }

    pub fn event_id(&self) -> FloEventId {
        FloEventId::new(self.actor_id, self.event_counter)
    }

    pub fn parent_id(&self) -> Option<FloEventId> {
        if self.parent_counter > 0 {
            Some(FloEventId::new(self.parent_actor_id, self.parent_counter))
        } else {
            None
        }
    }

    pub fn timestamp(&self) -> Timestamp {
        ::event::time::from_millis_since_epoch(self.timestamp)
    }
}

pub fn read_header<R: Read>(reader: &mut R) -> Result<EventHeader, io::Error> {
    let mut buffer = [0; 44];
    reader.read_exact(&mut buffer[..])?;

    if &buffer[..8] == ::engine::event_store::fs::FLO_EVT.as_bytes() {
        Ok(EventHeader{
            total_size: BigEndian::read_u32(&buffer[8..12]),
            event_counter: BigEndian::read_u64(&buffer[12..20]),
            actor_id: BigEndian::read_u16(&buffer[20..22]),
            parent_counter: BigEndian::read_u64(&buffer[22..30]),
            parent_actor_id: BigEndian::read_u16(&buffer[30..32]),
            timestamp: BigEndian::read_u64(&buffer[32..40]),
            namespace_length: BigEndian::read_u32(&buffer[40..44]),
        })
    } else {
        Err(invalid_bytes_err(format!("expected {:?}, got: {:?}", ::engine::event_store::fs::FLO_EVT.as_bytes(), &buffer[..8])))
    }
}

pub fn read_event<R: Read>(reader: &mut R) -> Result<OwnedFloEvent, io::Error> {
    read_header(reader).and_then(|header| {
        let mut namespace_buffer = vec![0; header.namespace_length as usize];
        reader.read_exact(&mut namespace_buffer).and_then(move |_ns_read| {
            String::from_utf8(namespace_buffer).map_err(|err| {
                invalid_bytes_err(format!("namespace contained invalid utf8 character: {:?}", err))
            })
        }).and_then(|namespace| {
            let mut data_len_buffer = [0; 4];
            reader.read_exact(&mut data_len_buffer).and_then(|()| {
                let data_length = BigEndian::read_u32(&data_len_buffer);
                debug_assert_eq!(data_length as usize, header.compute_data_length());
                let mut data_buffer = vec![0; data_length as usize];
                reader.read_exact(&mut data_buffer).map(|()| {
                    OwnedFloEvent::new(header.event_id(), header.parent_id(), header.timestamp(), namespace, data_buffer)
                })
            })
        })
    })
}

fn invalid_bytes_err(error_desc: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error_desc)
}

