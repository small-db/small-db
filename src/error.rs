use std::{error::Error, fmt};

use backtrace::Backtrace;

#[derive(Debug)]
pub struct SmallError {
    details: String,
}

impl SmallError {
    pub fn new(msg: &str) -> SmallError {
        // panic!("msg: [{}]", msg);

        let bt = Backtrace::new();
        // error!("msg: [{}], backtrace: {:?}", msg, bt);

        let details = format!("msg: [{}], backtrace: {:?}", msg, bt);

        // let details = "abc\n123".to_string();

        SmallError { details }
    }
}

impl fmt::Display for SmallError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for SmallError {
    fn description(&self) -> &str {
        &self.details
    }
}
