use crate::{error::SmallError, storage::tuple::Tuple};

pub struct Batch {
    pub rows: Vec<Tuple>,
}

/// ExprState represents the evaluation state for a whole expression tree.
pub trait Stream {
    fn next_batch(&mut self) -> Result<Option<Batch>, SmallError>;
}
