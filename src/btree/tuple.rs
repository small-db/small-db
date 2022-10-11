use crate::field::*;
use std::{
    fmt::{self},
    usize,
};

use super::page::BTreePageID;

#[derive(Debug, Default)]
pub struct Tuple {
    pub scheme: TupleScheme,
    pub fields: Vec<IntField>,
}

impl Tuple {
    pub fn new(scheme: TupleScheme, bytes: &[u8]) -> Tuple {
        let mut cells: Vec<IntField> = Vec::new();
        let mut start: usize = 0;
        let mut end: usize = 0;
        for field in &scheme.fields {
            match field.field_type {
                Type::INT => {
                    end += get_type_length(field.field_type);
                    let cell_bytes = &bytes[start..end];

                    let mut bytes_array = [0; 4];
                    for i in 0..4 {
                        bytes_array[i] = cell_bytes[i];
                    }
                    let value = i32::from_be_bytes(bytes_array);

                    cells.push(IntField::new(value));

                    start = end;
                }
            }
        }
        Tuple {
            scheme,
            fields: cells,
        }
    }

    pub fn new_default_tuple(scheme: TupleScheme, _width: usize) -> Tuple {
        let mut cells: Vec<IntField> = Vec::new();
        for field in &scheme.fields {
            match field.field_type {
                Type::INT => {
                    cells.push(IntField::new(0));
                }
            }
        }
        Tuple {
            scheme,
            fields: cells,
        }
    }

    pub fn new_btree_tuple(value: i32, width: usize) -> Tuple {
        let scheme = simple_int_tuple_scheme(width, "");
        let _bytes = [0];
        let mut tuple = Tuple::new_default_tuple(scheme, width);
        for i in 0..tuple.fields.len() {
            tuple.set_field(i, IntField::new(value));
        }
        tuple
    }

    pub fn set_field(&mut self, i: usize, c: IntField) {
        self.fields[i] = c;
    }

    pub fn get_field(&self, i: usize) -> IntField {
        self.fields[i]
    }

    pub fn clone(&self) -> Tuple {
        Tuple {
            scheme: self.scheme.clone(),
            fields: self.fields.to_vec(),
        }
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug)]
pub struct TupleScheme {
    pub fields: Vec<FieldItem>,
}

impl PartialEq for TupleScheme {
    fn eq(&self, other: &Self) -> bool {
        let matching = self
            .fields
            .iter()
            .zip(&other.fields)
            .filter(|&(a, b)| a == b)
            .count();
        self.fields.len() == matching
    }
}

impl TupleScheme {
    pub fn merge(scheme1: TupleScheme, scheme2: TupleScheme) -> TupleScheme {
        let mut new_scheme = TupleScheme {
            ..Default::default()
        };

        for f in scheme1.fields {
            new_scheme.fields.push(f);
        }
        for f in scheme2.fields {
            new_scheme.fields.push(f);
        }

        new_scheme
    }

    /// get tuple size in bytes
    pub fn get_size(&self) -> usize {
        self.fields.len() * 4
    }
}

impl Clone for TupleScheme {
    fn clone(&self) -> Self {
        Self {
            fields: self.fields.to_vec(),
        }
    }
}

impl Default for TupleScheme {
    fn default() -> TupleScheme {
        TupleScheme { fields: Vec::new() }
    }
}

pub fn simple_int_tuple_scheme(width: usize, name_prefix: &str) -> TupleScheme {
    let mut fields: Vec<FieldItem> = Vec::new();
    for i in 0..width {
        let field = FieldItem {
            field_name: format!("{}-{}", name_prefix, i),
            field_type: Type::INT,
        };
        fields.push(field);
    }

    TupleScheme { fields: fields }
}

#[cfg(test)]
mod tests {
    use log::{debug, info};

    use crate::util::init_log;

    use super::*;

    #[test]
    fn test_tuple_clone() {
        init_log();

        let tuple = Tuple::new_btree_tuple(35, 2);
        debug!("tuple: {}", tuple);
        let new_tuple = tuple.clone();
        debug!("new tuple: {}", new_tuple);
    }
}
