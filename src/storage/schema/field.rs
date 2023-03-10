use std::fmt::{self, Debug};

use crate::{
    io::{Condensable, SmallReader, Vaporizable},
    Op,
};

// TODO: add CHAR type
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Type {
    INT,
    CHAR(i8),
}

pub fn get_type_length(t: Type) -> usize {
    match t {
        Type::INT => 4,
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
