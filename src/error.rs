use std::{error::Error, fmt};

use backtrace::Backtrace;

#[derive(Debug)]
pub struct SmallError {
    details: String,
}

impl SmallError {
    pub fn new(msg: &str) -> SmallError {
        // let bt = Backtrace::new();
        // let details = format!("msg: [{}], backtrace: {:?}", msg, bt);
        let details = format!("msg: [{}]", msg);
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
