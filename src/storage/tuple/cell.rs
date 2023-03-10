use std::fmt::{self, Debug};

use crate::{
    io::{Condensable, SmallReader, Vaporizable},
    Op,
};

pub trait Cell {}

#[derive(Copy, Clone, PartialEq, Eq, Ord, Debug, PartialOrd)]
pub struct IntCell {
    pub value: i32,
}

impl IntCell {
    pub fn new(v: i32) -> IntCell {
        IntCell { value: v }
    }

    pub fn len(&self) -> usize {
        4
    }

    pub fn compare(&self, op: Op, field: IntCell) -> bool {
        match op {
            crate::Op::Equals => self.value == field.value,
            crate::Op::GreaterThan => self.value > field.value,
            crate::Op::LessThan => self.value < field.value,
            crate::Op::LessThanOrEq => self.value <= field.value,
            crate::Op::GreaterThanOrEq => self.value >= field.value,
            crate::Op::Like => todo!(),
            crate::Op::NotEquals => self.value != field.value,
        }
    }
}

impl Condensable for IntCell {
    fn to_bytes(&self) -> Vec<u8> {
        self.value.to_be_bytes().to_vec()
    }
}

impl Vaporizable for IntCell {
    fn read_from(reader: &mut SmallReader) -> Self {
        let data = reader.read_exact(4);
        IntCell {
            value: i32::from_be_bytes([
                data[0], data[1], data[2], data[3],
            ]),
        }
    }
}

impl fmt::Display for IntCell {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}
