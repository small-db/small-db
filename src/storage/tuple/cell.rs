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

impl Encodeable for Cell {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Cell::Int32(v) => v.to_be_bytes().to_vec(),
            Cell::String(v) => v.as_bytes().to_vec(),
        }
    }
}

// #[derive(Copy, Clone, PartialEq, Eq, Ord, Debug, PartialOrd)]
// pub struct Cell {
//     pub value: i32,
// }

// impl Cell {
//     pub fn new(v: i32) -> Cell {
//         Cell { value: v }
//     }

//     pub fn len(&self) -> usize {
//         4
//     }

//     pub fn compare(&self, op: Op, field: Cell) -> bool {
//         match op {
//             crate::Op::Equals => self.value == field.value,
//             crate::Op::GreaterThan => self.value > field.value,
//             crate::Op::LessThan => self.value < field.value,
//             crate::Op::LessThanOrEq => self.value <= field.value,
//             crate::Op::GreaterThanOrEq => self.value >= field.value,
//             crate::Op::Like => todo!(),
//             crate::Op::NotEquals => self.value != field.value,
//         }
//     }
// }

// impl Condensable for Cell {
//     fn to_bytes(&self) -> Vec<u8> {
//         self.value.to_be_bytes().to_vec()
//     }
// }

// impl Vaporizable for Cell {
//     fn read_from(reader: &mut SmallReader) -> Self {
//         let data = reader.read_exact(4);
//         Cell {
//             value: i32::from_be_bytes([
//                 data[0], data[1], data[2], data[3],
//             ]),
//         }
//     }
// }

// impl fmt::Display for Cell {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{}", self.value)
//     }
// }
