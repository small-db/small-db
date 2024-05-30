use crate::error::SmallError;

pub struct Batch {}

/// ExprState represents the evaluation state for a whole expression tree.
pub trait Stream {
    fn next_batch(&mut self) -> Result<Option<Batch>, SmallError>;
}
