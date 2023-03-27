use std::convert::TryInto;

use crate::{
    io::{read_exact, Decodeable, Encodeable},
    storage::tuple::Cell,
};

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Type {
    Bool,
    Int64,
    Float64,
    Bytes(u8),
}

/// Cell generators
impl Type {
    pub fn new_cell_bytes(&self, v: &[u8]) -> Cell {
        match self {
            Type::Bytes(size) => {
                if v.len() > *size as usize {
                    panic!("bytes size too large");
                }

                Cell::Bytes(v.to_vec())
            }
            _ => panic!("not bytes"),
        }
    }
}

impl Type {
    /// Get the size of the type in bytes.
    pub fn get_disk_size(&self) -> usize {
        match self {
            Type::Bool => 1,
            Type::Int64 | Type::Float64 => 8,
            Type::Bytes(size) => {
                // The first two bytes is the size of the bytes.
                //
                // We use fixed size now to calculate the size of the
                // tuple.
                2 + *size as usize
            }
        }
    }
}

impl Encodeable for Type {
    fn encode(&self) -> Vec<u8> {
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
                vec![3, *size]
            }
        }
    }
}

impl Decodeable for Type {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Self {
        let bytes: [u8; 2] =
            read_exact(reader, 2).try_into().unwrap();

        match bytes {
            [0, 1] => Type::Bool,
            [1, 8] => Type::Int64,
            [2, 8] => Type::Float64,
            [3, size] => Type::Bytes(size),
            _ => panic!("invalid type"),
        }
    }
}
