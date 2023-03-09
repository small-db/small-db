use crate::storage::base::IntCell;

pub enum Op {
    Equals,
    GreaterThan,
    GreaterThanOrEq,
    LessThan,
    LessThanOrEq,
    Like,
    NotEquals,
}

pub struct Predicate {
    pub op: Op,
    pub field: IntCell,
}

impl Predicate {
    pub fn new(op: Op, field: IntCell) -> Self {
        Self { op, field }
    }
}
