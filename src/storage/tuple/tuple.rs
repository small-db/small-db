use std::{
    fmt::{self},
    io::Cursor,
    usize,
};

use crate::{
    btree::page::BTreePageID,
    io::{Decodeable, Encodeable},
    storage::{
        schema::{Schema, Type},
        tuple::Cell,
    },
};

// #[derive(Default)]
pub struct Tuple {
    cells: Vec<Cell>,
}

// constructors
impl Tuple {
    // TODO: remove this api
    pub fn new(scheme: &Schema, bytes: &[u8]) -> Self {
        let mut reader = Cursor::new(bytes);
        return Self::read_from(&mut reader, &scheme);
    }

    pub fn new_from_cells(cells: &Vec<Cell>) -> Self {
        Self {
            cells: cells.to_vec(),
        }
    }

    pub fn read_from<R: std::io::Read>(
        reader: &mut R,
        tuple_scheme: &Schema,
    ) -> Self {
        let mut cells: Vec<Cell> = Vec::new();
        for field in &tuple_scheme.fields {
            match field.t {
                Type::Bool => {
                    cells.push(Cell::Bool(bool::read_from(reader)));
                }
                Type::Int64 => {
                    cells.push(Cell::Int64(i64::read_from(reader)));
                }
                Type::Float64 => {
                    cells.push(Cell::Float64(f64::read_from(reader)));
                }
                Type::Bytes(len) => {
                    let mut bytes = Vec::new();
                    for _ in 0..len {
                        bytes.push(u8::read_from(reader));
                    }
                    cells.push(Cell::Bytes(bytes));
                }
            }
        }
        Tuple { cells }
    }

    // TODO: remove this api
    pub fn new_int_tuples(value: i64, width: usize) -> Self {
        let mut cells: Vec<Cell> = Vec::new();
        for _ in 0..width {
            cells.push(Cell::Int64(value));
        }

        Tuple { cells }
    }
}

impl Tuple {
    pub fn get_cell(&self, i: usize) -> Cell {
        self.cells[i].clone()
    }

    pub fn get_cells(&self) -> Vec<Cell> {
        self.cells.clone()
    }

    pub fn clone(&self) -> Tuple {
        Tuple {
            cells: self.cells.clone(),
        }
    }
}

impl Encodeable for Tuple {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for cell in &self.cells {
            let mut cell_bytes = cell.to_bytes();
            bytes.append(&mut cell_bytes);
        }
        bytes
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        for (i, field) in self.cells.iter().enumerate() {
            if field != &other.cells[i] {
                return false;
            }
        }

        return true;
    }
}

impl Eq for Tuple {}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut content: String = "{".to_owned();
        for cell in &self.cells {
            let cell_str = format!("{:?}, ", cell);
            content.push_str(&cell_str);
        }
        content = content[..content.len() - 2].to_string();
        content.push_str(&"}");
        write!(f, "{}", content,)
    }
}

impl fmt::Debug for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

// TODO: move this to `btree` module, or remove it
#[derive(PartialEq)]
pub struct WrappedTuple {
    internal: Tuple,
    slot_number: usize,
    pid: BTreePageID,
}

impl std::ops::Deref for WrappedTuple {
    type Target = Tuple;
    fn deref(&self) -> &Self::Target {
        &self.internal
    }
}

impl std::ops::DerefMut for WrappedTuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.internal
    }
}

impl WrappedTuple {
    pub fn new(
        internal: Tuple,
        slot_number: usize,
        pid: BTreePageID,
    ) -> WrappedTuple {
        WrappedTuple {
            internal,
            slot_number,
            pid,
        }
    }

    pub fn get_slot_number(&self) -> usize {
        self.slot_number
    }

    pub fn get_pid(&self) -> BTreePageID {
        self.pid
    }
}

impl Eq for WrappedTuple {}

impl fmt::Display for WrappedTuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.cells)
    }
}

impl fmt::Debug for WrappedTuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[cfg(test)]
mod tests {
    use log::debug;

    use super::*;
    use crate::utils::init_log;

    #[test]
    fn test_tuple_clone() {
        init_log();

        let tuple = Tuple::new_int_tuples(35, 2);
        debug!("tuple: {}", tuple);
        let new_tuple = tuple.clone();
        debug!("new tuple: {}", new_tuple);
    }
}
