
use rotor::Notifier;
use event::EventId;
use queryst;
use serde_json::Value;


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

pub fn get_last_event_id(url_path: &str) -> Result<EventId, String> {
    let query = url_path.find('?').map(|idx| {
        url_path.split_at(idx + 1).1
    }).unwrap_or(url_path);

    queryst::parse(query).map_err(|_e| {
        "wtf, mate?".to_string()
    }).and_then(|params| {
        // params.find(param_name).map(|val| {
        //     val.as_u64().ok_or(&format!("{} must be a positive integer", param_name))
        // })
        map_to_result(&params)
    })
}

fn map_to_result(params: &Value) -> Result<EventId, String> {
    let param_name = "lastEvent";

    params.find(param_name).map(|param_value| {
        param_value.as_string().unwrap().parse::<u64>()
                .map_err(|_parse_int_err| {
                    format!("parameter '{}' must be a positive integer, got: {:?}", param_name, param_value)
                })
    }).unwrap_or(Ok(0))
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
