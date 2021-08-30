use std::fmt::Debug;

use crate::Predicate;

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

    pub fn satisfy(&self, predicate: &Predicate) -> bool {
        match predicate.op {
            crate::Op::Equals => self.value == predicate.field.value,
            crate::Op::GreaterThan => self.value > predicate.field.value,
            crate::Op::LessThan => self.value < predicate.field.value,
            crate::Op::LessThanOrEq => self.value <= predicate.field.value,
            crate::Op::GreaterThanOrEq => self.value >= predicate.field.value,
            crate::Op::Like => todo!(),
            crate::Op::NotEquals => self.value != predicate.field.value,
        }
    }
}
