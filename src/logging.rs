use log4rs::config::{Config, Appender, Logger, Root};
use log4rs::append::console::ConsoleAppender;
use log4rs::init_config;

use log::LogLevelFilter;

use std::boxed::Box;

const STD_OUT_APPENDER: &'static str = "stdout";

pub fn init_logging() {
    let console_appender = ConsoleAppender::builder().build();
    let appender = Appender::builder().build(STD_OUT_APPENDER.to_string(), Box::new(console_appender));

    let root = Root::builder().appender(STD_OUT_APPENDER.to_string()).build(LogLevelFilter::Info);

    // to work around: https://github.com/tailhook/rotor-http/issues/42
    let rotor_logger = Logger::builder().build("rotor_http::server::parser".to_string(), LogLevelFilter::Error);
    let flo_logger = Logger::builder().build("flo".to_string(), LogLevelFilter::Trace);

    let config = Config::builder()
            .appender(appender)
            .logger(rotor_logger)
            .logger(flo_logger)
            .build(root).unwrap();
    init_config(config);
}
