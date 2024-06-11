use std::{error::Error, fmt};

use backtrace::Backtrace;
use log::{debug, info};

#[derive(Debug)]
pub struct SmallError {
    details: String,
}

impl SmallError {
    pub(crate) fn new(msg: &str) -> SmallError {
        let bt = Backtrace::new();
        let details = format!("msg: [{}]\nerror backtrace:\n{:?}", msg, bt);
        SmallError { details }
    }

    pub(crate) fn show_backtrace(&self) {
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

/// Get the line number and file name of the caller
pub(crate) fn get_caller() -> String {
    let bt = Backtrace::new();
    let frames = bt.frames();

    let frame = frames.iter().nth(6).unwrap();
    let symbol = frame.symbols().iter().next().unwrap();
    format!(
        "{}:{}",
        symbol.filename().unwrap().to_string_lossy(),
        symbol.lineno().unwrap()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error() {
        let err = SmallError::new("test error");
        err.show_backtrace();
    }

    #[test]
    fn test_caller() {
        fn foo() {
            let caller = get_caller();
            println!("{}", caller);
        }

        foo();
    }
}
