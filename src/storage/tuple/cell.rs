use std::fmt::{self, Debug};

use crate::{
    io::{Decodeable, Encodeable, SmallReader},
    Op,
};

#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Int32(i32),
    String(String),
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cell::Int32(a), Cell::Int32(b)) => a == b,
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
            (Cell::Int32(a), Cell::Int32(b)) => a.partial_cmp(b),
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
            Cell::Null => vec![0],
            Cell::Int32(v) => v.to_be_bytes().to_vec(),
            Cell::String(v) => v.as_bytes().to_vec(),
        }
    }
}
