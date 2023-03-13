use std::fmt::Debug;

// TODO: add CHAR type
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Type {
    Int64,
    Float64,
    CHAR(i8),
}

pub fn get_type_length(t: Type) -> usize {
    match t {
        Type::Int64 | Type::Float64 => 8,
        Type::CHAR(size) => size as usize,
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct FieldItem {
    pub field_name: String,
    pub field_type: Type,
}

impl FieldItem {
    pub fn new(field_name: &str, field_type: Type) -> FieldItem {
        FieldItem {
            field_type,
            field_name: field_name.to_string(),
        }
    }
}
