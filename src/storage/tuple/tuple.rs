use std::{
    fmt::{self},
    usize,
};

use crate::{
    btree::page::BTreePageID,
    io::{Condensable, SmallReader, Vaporizable},
    storage::{
        schema::{small_int_schema, Schema, Type},
        tuple::IntCell,
    },
};

#[derive(Default)]
pub struct Tuple {
    pub scheme: Schema,
    pub fields: Vec<IntCell>,
}

impl Tuple {
    // TODO: remove this api
    pub fn new(scheme: Schema, bytes: &[u8]) -> Self {
        let mut reader = SmallReader::new(bytes);
        return Self::read_from(&mut reader, &scheme);
    }

    // TODO: remove this api
    pub fn new_default_tuple(scheme: Schema, _width: usize) -> Self {
        let mut cells: Vec<IntCell> = Vec::new();
        for field in &scheme.fields {
            match field.field_type {
                Type::INT => {
                    cells.push(IntCell::new(0));
                }
                Type::CHAR(_) => {
                    todo!()
                }
            }
        }
        Tuple {
            scheme,
            fields: cells,
        }
    }

    pub fn new_btree_tuple(value: i32, width: usize) -> Tuple {
        let scheme = small_int_schema(width, "");
        let _bytes = [0];
        let mut tuple = Tuple::new_default_tuple(scheme, width);
        for i in 0..tuple.fields.len() {
            tuple.set_field(i, IntCell::new(value));
        }
        tuple
    }

    pub fn set_field(&mut self, i: usize, c: IntCell) {
        self.fields[i] = c;
    }

    pub fn get_field(&self, i: usize) -> IntCell {
        self.fields[i]
    }

    pub fn clone(&self) -> Tuple {
        Tuple {
            scheme: self.scheme.clone(),
            fields: self.fields.to_vec(),
        }
    }

    pub fn read_from(
        reader: &mut crate::io::SmallReader,
        tuple_scheme: &Schema,
    ) -> Self {
        let mut cells: Vec<IntCell> = Vec::new();
        for field in &tuple_scheme.fields {
            match field.field_type {
                Type::INT => {
                    cells.push(IntCell::read_from(reader));
                }
                Type::CHAR(len) => {
                    let mut bytes = Vec::new();
                    for _ in 0..len {
                        bytes.push(reader.read::<u8>());
                    }
                    cells.push(IntCell::new(0));
                }
            }
        }
        Tuple {
            scheme: tuple_scheme.clone(),
            fields: cells,
        }
    }
}

impl Condensable for Tuple {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for cell in &self.fields {
            let mut cell_bytes = cell.to_bytes();
            bytes.append(&mut cell_bytes);
        }
        bytes
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        if self.scheme != other.scheme {
            return false;
        }

        for (i, field) in self.fields.iter().enumerate() {
            if field != &other.fields[i] {
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
        for cell in &self.fields {
            let cell_str = format!("{}, ", cell.value);
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
        let mut content: String = "{".to_owned();
        for cell in &self.fields {
            let cell_str = format!("{}, ", cell.value);
            content.push_str(&cell_str);
        }
        content = content[..content.len() - 2].to_string();
        content.push_str(&"}");
        write!(f, "{}", content,)
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

        let tuple = Tuple::new_btree_tuple(35, 2);
        debug!("tuple: {}", tuple);
        let new_tuple = tuple.clone();
        debug!("new tuple: {}", new_tuple);
    }
}
