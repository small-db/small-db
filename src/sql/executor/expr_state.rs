use crate::error::SmallError;

pub struct Batch {}

/// ExprState represents the evaluation state for a whole expression tree.
pub struct ExprState {}

impl ExprState {
    pub fn new() -> Self {
        Self {}
    }

    pub fn next_batch(&mut self) -> Result<Batch, SmallError> {
        todo!()
    }
}
