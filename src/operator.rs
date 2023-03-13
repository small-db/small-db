use crate::storage::tuple::Cell;

#[derive(Clone)]
pub enum Op {
    Equals,
    GreaterThan,
    GreaterThanOrEq,
    LessThan,
    LessThanOrEq,
    Like,
    NotEquals,
}

#[derive(Clone)]
pub struct Predicate {
    pub op: Op,
    pub field: Cell,
}

impl Predicate {
    pub fn new(op: Op, field: Cell) -> Self {
        Self { op, field }
    }
}
