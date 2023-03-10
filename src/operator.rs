use crate::storage::tuple::Cell;

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
    pub field: Cell,
}

impl Predicate {
    pub fn new(op: Op, field: Cell) -> Self {
        Self { op, field }
    }
}
