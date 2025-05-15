use std::path::Path;

pub(crate) struct WALManager {}

impl WALManager {
    pub(crate) fn new<P: AsRef<Path> + Clone>(file_path: &P) -> Self {
        Self {}
    }
}
