use crate::error::SmallError;

pub struct Batch {}

/// ExprState represents the evaluation state for a whole expression tree.
pub struct Stream {}

impl Stream {
    pub fn new() -> Self {
        Self {}
    }

    pub fn next_batch(&mut self) -> Result<Batch, SmallError> {
        todo!()
    }
}
