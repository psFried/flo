extern crate flo;
extern crate url;


use url::Url;
use flo::client::*;



#[test]
fn this_is_only_a_test() {
    let url = Url::parse("http://localhost:3000").unwrap();
    let producer = FloProducer::new(url);
    let result = producer.emit_raw(r#"{"myKey": "what a great value!"}"#);
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap());
}
