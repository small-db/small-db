use std::fmt::Debug;

use crate::io::{Decodeable, Encodeable, SmallReader};

// TODO: add CHAR type
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Type {
    Bool,
    Int64,
    Float64,
    Bytes(u8),
}

impl Type {
    pub fn len(&self) -> usize {
        match self {
            Type::Bool => 1,
            Type::Int64 | Type::Float64 => 8,
            Type::Bytes(size) => 1 + *size as usize,
        }
    }
}

impl Encodeable for Type {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Type::Bool => {
                vec![0, 1]
            }
            Type::Int64 => {
                vec![1, 8]
            }
            Type::Float64 => {
                vec![2, 8]
            }
            Type::Bytes(size) => {
                vec![4, *size]
            }
        }
    }
}

impl Decodeable for Type {
    fn read_from(reader: &mut SmallReader) -> Self {
        let bytes = reader.read_exact(2);

        match bytes {
            [0, 1] => Type::Bool,
            [1, 8] => Type::Int64,
            [2, 8] => Type::Float64,
            [4, size] => Type::Bytes(*size),
            _ => panic!("invalid type"),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Field {
    pub name: String,
    pub t: Type,
    pub is_primary: bool,
}

impl Field {
    pub fn new(
        field_name: &str,
        field_type: Type,
        is_primary: bool,
    ) -> Field {
        Field {
            t: field_type,
            name: field_name.to_string(),
            is_primary,
        }
    }
}
