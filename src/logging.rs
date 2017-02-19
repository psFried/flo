use log4rs::config::{Config, Appender, Logger, Root};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::append::Append;
use log4rs::init_config;

use log::{LogLevelFilter, LogLevel};

use std::boxed::Box;
use std::str::FromStr;
use std::path::PathBuf;

const LOG_APPENDER: &'static str = "log_appender";

pub enum LogFileOption {
    File(PathBuf),
    Stdout
}

#[derive(Debug, PartialEq, Clone)]
pub struct LogLevelOption {
    module: String,
    log_level: LogLevel
}

impl FromStr for LogLevelOption {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts: Vec<&str>  = s.split("=").collect();
        if parts.len() != 2 {
            return Err(format!("Invalid Log Option: '{}', must be in the format <module>=<level> (with exactly one '=')", s));
        }

        LogLevel::from_str(parts[1]).map_err(|_| {
            format!("invalid log level: '{}'", parts[1])
        }).map(|level| {
            LogLevelOption {
                module: parts.remove(0).to_owned(), // safe call since length was checked
                log_level: level
            }
        })
    }
}

pub fn init_logging(log_dest: LogFileOption, levels: Vec<LogLevelOption>) {
    let appender = match log_dest {
        LogFileOption::Stdout => {
            Box::new(ConsoleAppender::builder().build()) as Box<Append>
        }
        LogFileOption::File(path_buf) => {
            let appender = FileAppender::builder().build(path_buf).expect("Unable to open log file");
            Box::new(appender) as Box<Append>
        }
    };
    let appender = Appender::builder().build(LOG_APPENDER.to_string(), appender);
    //TODO: way to get name of module dynamically
    let default_server_logger = Logger::builder().additive(true).build("flo".to_owned(), LogLevelFilter::Info);
    let mut config = Config::builder().appender(appender).logger(default_server_logger);

    for level_opt in levels {
        config = config.logger(Logger::builder().additive(true).build(level_opt.module, level_opt.log_level.to_log_level_filter()));
    }

    let root = Root::builder().appender(LOG_APPENDER.to_string()).build(LogLevelFilter::Warn);
    let config = config.build(root).unwrap();
    init_config(config).unwrap();
}

#[cfg(test)]
mod test {
    use super::*;
    use log::LogLevel;

    fn test_valid_log_option(module: &str, level_str: &str, level: LogLevel) {
        let input = format!("{}={}", module, level_str);

        let result = LogLevelOption::from_str(&input).expect("failed to create log option");
        let expected = LogLevelOption {
            module: module.to_owned(),
            log_level: level
        };
        assert_eq!(expected, result);
    }

    fn test_invalid_log_option(input: &str) {
        let result = LogLevelOption::from_str(input);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_log_options_return_err_when_creating_from_str() {
        test_invalid_log_option("no equals sign");
        test_invalid_log_option("multiple=equals=trace");
        test_invalid_log_option("foo::bar=invalid");
    }

    #[test]
    fn log_option_is_created_from_str_with_valid_log_level() {
        test_valid_log_option("my::module", "trace", LogLevel::Trace);
        test_valid_log_option("my::module", "TRACE", LogLevel::Trace);
        test_valid_log_option("my::module", "TraCe", LogLevel::Trace);
        test_valid_log_option("my::module", "debug", LogLevel::Debug);
        test_valid_log_option("my::module", "Debug", LogLevel::Debug);
        test_valid_log_option("my::module", "info", LogLevel::Info);
        test_valid_log_option("my::module", "iNFo", LogLevel::Info);
        test_valid_log_option("my::module", "warn", LogLevel::Warn);
        test_valid_log_option("my::module", "WARN", LogLevel::Warn);
        test_valid_log_option("my::module", "error", LogLevel::Error);
        test_valid_log_option("my::module", "erroR", LogLevel::Error);
    }
}
