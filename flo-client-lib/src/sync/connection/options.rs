use event::{FloEventId, VersionVector};

/// Represents the information needed to start consuming events from the event stream. `ConsumerOptions` implements `Default`,
/// which allows you to easily specify only the options you wish to set explicitely
///
/// # Examples
///
/// ```
/// use flo_client_lib::sync::connection::ConsumerOptions;
/// use flo_client_lib::VersionVector;
///
/// let options = ConsumerOptions{max_events: 5, ..Default::default()};
/// assert_eq!(5, options.max_events);
/// assert_eq!("/**/*", &options.namespace);
/// assert!(!options.await_new_events);
/// assert_eq!(VersionVector::new(), options.version_vector);
/// ```
///
#[derive(Debug, PartialEq, Clone)]
pub struct ConsumerOptions {
    pub namespace: String,
    pub version_vector: VersionVector,
    pub max_events: u64,
    pub await_new_events: bool,
}

impl ConsumerOptions {
    pub fn simple<S: Into<String>>(namespace: S, start_position: FloEventId, max_events: u64) -> ConsumerOptions {
        let mut vv = VersionVector::new();
        vv.update_if_greater(start_position);
        ConsumerOptions::new(namespace, vv, max_events, false)
    }

    pub fn from_beginning<S: Into<String>>(namespace: S, max_events: u64) -> ConsumerOptions {
        ConsumerOptions::new(namespace, VersionVector::new(), max_events, false)
    }

    pub fn tail<S: Into<String>>(namespace: S, start_position: VersionVector) -> ConsumerOptions {
        ConsumerOptions::new(namespace, start_position, ::std::u64::MAX, true)
    }

    pub fn new<S: Into<String>>(namespace: S, start_position: VersionVector, max_events: u64, await_new: bool) -> ConsumerOptions {
        ConsumerOptions {
            namespace: namespace.into(),
            version_vector: start_position,
            max_events: max_events,
            await_new_events: await_new,
        }
    }
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        ConsumerOptions::new(::ALL_EVENTS_GLOB, VersionVector::new(), ::std::u64::MAX, false)
    }
}


