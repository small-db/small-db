use std::fmt;

use crate::storage::tuple::Cell;

#[derive(Clone, Debug)]
pub enum Op {
    Equals,
    GreaterThan,
    GreaterThanOrEq,
    LessThan,
    LessThanOrEq,
    Like,
    NotEquals,
}
