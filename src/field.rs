use std::fmt::Debug;
// use std::intrinsics::type_id;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Type {
    INT,
    STRING,
}

pub fn get_type_length(t: Type) -> usize {
    match t {
        Type::INT => 4,
        _ => 0,
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct FieldItem {
    pub(crate) field_type: Type,
    pub field_name: String,
}

#[derive(Copy, Clone, PartialEq, Debug, PartialOrd)]
pub struct IntField {
    pub value: i32,
}

impl IntField {
    pub fn new(v: i32) -> IntField {
        IntField { value: v }
    }
}
