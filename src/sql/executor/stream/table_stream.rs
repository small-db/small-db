use crate::error::SmallError;

use super::{Batch, Stream};

pub struct TableStream {}

impl TableStream {
    pub fn new() -> Self {
        Self {}
    }
}

impl Stream for TableStream {
    fn next_batch(&mut self) -> Result<Option<Batch>, SmallError> {
        todo!()
    }
}
