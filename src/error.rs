use std::{error::Error, fmt};

use backtrace::Backtrace;

#[derive(Debug)]
pub struct SmallError {
    details: String,
}

impl SmallError {
    pub fn new(msg: &str) -> SmallError {
        let bt = Backtrace::new();
        let details = format!("msg: [{}]\nerror backtrace: {:?}", msg, bt);
        SmallError { details }
    }

    pub fn show_backtrace(&self) {
        println!("{}", self.details);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error() {
        let err = SmallError::new("test error");
        err.show_backtrace();
    }
}
