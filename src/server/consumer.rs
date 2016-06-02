
use rotor::{Scope, Time, Notifier};
use rotor_http::server::{RecvMode, Head, Response};
use event::EventId;
use queryst;
use serde_json::Value;
use context::FloContext;
use event_store::EventStore;
use super::{get_namespace_from_path, FloServer};
use std::time::Duration;


pub trait ConsumerNotifier: Sized {

    fn notify(&mut self);

}


pub struct RotorConsumerNotifier {
    notifier: Notifier,
}

impl RotorConsumerNotifier {
    pub fn new(notifier: Notifier) -> RotorConsumerNotifier {
        RotorConsumerNotifier {
            notifier: notifier,
        }
    }
}

impl ConsumerNotifier for RotorConsumerNotifier {

    fn notify(&mut self) {
        self.notifier.wakeup().unwrap();
    }
}

pub const PARAM_LAST_EVENT_ID: &'static str = "lastEvent";

pub fn get_last_event_id(url_path: &str) -> Result<EventId, String> {
    let query = url_path.find('?').map(|idx| {
        url_path.split_at(idx + 1).1
    }).unwrap_or(url_path);

    queryst::parse(query).map_err(|_e| {
        "wtf, mate?".to_string()
    }).and_then(|params| {
        get_last_event_id_from_params(&params)
    })
}

fn get_last_event_id_from_params(params: &Value) -> Result<EventId, String> {
    params.find(PARAM_LAST_EVENT_ID).map(|param_value| {
        param_value.as_string().unwrap().parse::<u64>()
                .map_err(|_parse_int_err| {
                    format!("parameter '{}' must be a positive integer, got: {:?}",
                            PARAM_LAST_EVENT_ID,
                            param_value)
                })
    }).unwrap_or(Ok(0))
}

pub fn init_consumer<S: EventStore>(request: Head,
        response: &mut Response,
        scope: &mut Scope<FloContext<RotorConsumerNotifier, S>>)
        -> Option<(FloServer, RecvMode, Time)> {
	
	let namespace = get_namespace_from_path(request.path);

    let last_event_id = get_last_event_id(request.path).unwrap();
    let notifier = scope.notifier();
    let consumer_id = scope.add_consumer_to_namespace(RotorConsumerNotifier::new(notifier), last_event_id, &namespace);

    response.status(200u16, "Success");
    response.add_chunked().unwrap();
    response.done_headers().unwrap();

    Some((FloServer::Consumer(consumer_id),
            RecvMode::Buffered(1024),
            scope.now() + Duration::new(30, 0)))
}

pub fn on_wakeup<C, S>(consumer_id: usize, response: &mut Response, context: &mut FloContext<C, S>) where C: ConsumerNotifier, S: EventStore {

    trace!("Waking up consumer: {}", consumer_id);
    context.get_next_event(consumer_id).map(|event| {
        trace!("writing to consumer: {:?}", event.data);
        response.write_body(event.get_raw_bytes());
        // response.write_body(b"\r\n");
        event.get_id()
    }).map(|event_id| {
        context.confirm_event_written(consumer_id, event_id);
    });
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[allow(non_snake_case)]
    fn get_last_event_id_returns_value_of_lastEvent_query_param() {

        macro_rules! assert_event_id_is {
            ($expected:expr, $path:expr) => (
                {
                    let result = get_last_event_id($path);
                    let actual = result.expect(&format!("parsing: '{}' returned error", $path));
                    assert_eq!($expected, actual);
                }
            )
        }
        assert_event_id_is!(4, "/path?lastEvent=4");
        assert_event_id_is!(987654, "/path?lastEvent=987654");
        assert_event_id_is!(0, "/path?lastEvent=0");
        assert_event_id_is!(0, "/path");
    }

    #[test]
    #[allow(non_snake_case)]
    fn get_last_event_id_returns_err_if_lastEvent_param_is_not_a_positive_integer() {
        assert_eq!("parameter \'lastEvent\' must be a positive integer, got: \"kdfk\"".to_string(), get_last_event_id("/path?lastEvent=kdfk").unwrap_err());
        assert_eq!("parameter \'lastEvent\' must be a positive integer, got: \"-67\"".to_string(), get_last_event_id("/path?lastEvent=-67").unwrap_err());
    }

}
