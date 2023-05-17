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
    pub field_index: usize,
    pub op: Op,
    pub cell: Cell,
}

impl Predicate {
    pub fn new(field_index: usize, op: Op, cell: &Cell) -> Self {
        Self {
            field_index,
            op,
            cell: cell.clone(),
        }
    }
}
