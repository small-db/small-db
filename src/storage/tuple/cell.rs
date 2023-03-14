use std::fmt::Debug;

use crate::io::Encodeable;

#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cell::Int64(a), Cell::Int64(b)) => a == b,
            (Cell::String(a), Cell::String(b)) => a == b,
            _ => todo!(),
        }
    }
}

impl PartialOrd for Cell {
    fn partial_cmp(
        &self,
        other: &Self,
    ) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Cell::Int64(a), Cell::Int64(b)) => a.partial_cmp(b),
            (Cell::String(a), Cell::String(b)) => a.partial_cmp(b),
            _ => todo!(),
        }
    }
}

impl Eq for Cell {}

impl Ord for Cell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Encodeable for Cell {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Cell::Null => todo!(),
            Cell::Bool(v) => vec![*v as u8],
            Cell::Int64(v) => v.to_be_bytes().to_vec(),
            Cell::Float64(v) => v.to_be_bytes().to_vec(),
            Cell::String(v) => v.as_bytes().to_vec(),
        }
    }
}
