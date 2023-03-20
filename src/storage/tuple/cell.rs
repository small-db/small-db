use std::fmt::Debug;

use crate::{error::SmallError, io::Encodeable};

#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Char(String),
    Bytes(Vec<u8>),
}

impl Cell {
    pub fn new_bool(v: bool) -> Self {
        Cell::Bool(v)
    }

    pub fn get_bool(&self) -> Result<bool, SmallError> {
        match self {
            Cell::Bool(v) => Ok(*v),
            _ => Err(SmallError::new("not bool")),
        }
    }

    pub fn new_int64(v: i64) -> Self {
        Cell::Int64(v)
    }

    pub fn get_int64(&self) -> Result<i64, SmallError> {
        match self {
            Cell::Int64(v) => Ok(*v),
            _ => Err(SmallError::new("not int64")),
        }
    }

    pub fn new_float64(v: f64) -> Self {
        Cell::Float64(v)
    }

    pub fn get_float64(&self) -> Result<f64, SmallError> {
        match self {
            Cell::Float64(v) => Ok(*v),
            _ => Err(SmallError::new("not float64")),
        }
    }

    pub fn new_string(v: &str) -> Self {
        Cell::Char(v.to_string())
    }

    pub fn get_string(&self) -> Result<String, SmallError> {
        match self {
            Cell::Char(v) => Ok(v.clone()),
            _ => Err(SmallError::new("not string")),
        }
    }

    pub fn new_bytes(v: &[u8]) -> Self {
        Cell::Bytes(v.to_vec())
    }

    pub fn get_bytes(&self) -> Result<Vec<u8>, SmallError> {
        match self {
            Cell::Bytes(v) => Ok(v.clone()),
            _ => Err(SmallError::new("not bytes")),
        }
    }
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cell::Int64(a), Cell::Int64(b)) => a == b,
            (Cell::Char(a), Cell::Char(b)) => a == b,
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
            (Cell::Char(a), Cell::Char(b)) => a.partial_cmp(b),
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
            Cell::Char(v) => v.as_bytes().to_vec(),
            Cell::Bytes(v) => v.clone(),
        }
    }
}
