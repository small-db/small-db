use crate::io::{Decodeable, Encodeable, SmallReader};

const MAX_BYTES_SIZE: usize = 100;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Type {
    Bool,
    Int64,
    Float64,
    Bytes(u8),
}

impl Type {
    /// Get the size of the type in bytes.
    pub fn size(&self) -> usize {
        match self {
            Type::Bool => 1,
            Type::Int64 | Type::Float64 => 8,
            Type::Bytes(size) => {
                // The first byte is the size of the bytes.
                //
                // We use fixed size now to calculate the size of the
                // tuple.
                1 + *size as usize
            }
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
                vec![3, *size]
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
            [3, size] => Type::Bytes(*size),
            _ => panic!("invalid type"),
        }
    }
}
