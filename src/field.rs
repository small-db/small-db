use std::fmt::{self, Debug};

use crate::{Op, Predicate};

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Type {
    INT,
}

pub fn get_type_length(t: Type) -> usize {
    match t {
        Type::INT => 4,
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct FieldItem {
    pub field_type: Type,
    pub field_name: String,
}

#[derive(Copy, Clone, PartialEq, Eq, Ord, Debug, PartialOrd)]
pub struct IntField {
    pub value: i32,
}

impl IntField {
    pub fn new(v: i32) -> IntField {
        IntField { value: v }
    }

    pub fn len(&self) -> usize {
        4
    }

    pub fn compare(&self, op: Op, field: IntField) -> bool {
        match op {
            crate::Op::Equals => self.value == field.value,
            crate::Op::GreaterThan => self.value > field.value,
            crate::Op::LessThan => self.value < field.value,
            crate::Op::LessThanOrEq => self.value <= field.value,
            crate::Op::GreaterThanOrEq => self.value >= field.value,
            crate::Op::Like => todo!(),
            crate::Op::NotEquals => self.value != field.value,
        }
    }
}

impl fmt::Display for IntField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}
