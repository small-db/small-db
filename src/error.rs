use std::{error::Error, fmt};

use backtrace::Backtrace;

#[derive(Debug)]
pub struct SmallError {
    msg: String,
    backtrace: String,
}

impl SmallError {
    pub(crate) fn new(msg: &str) -> SmallError {
        let bt = Backtrace::new();
        SmallError {
            msg: msg.to_string(),
            backtrace: format!("error backtrace:\n{:?}", bt),
        }
    }

    pub fn show_backtrace(&self) {
        println!("error: {}\n{}", self.msg, self.backtrace);
    }
}

impl fmt::Display for SmallError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error for SmallError {
    fn description(&self) -> &str {
        &self.msg
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
