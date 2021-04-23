use crate::field::*;
use std::{cell::RefCell, fmt::{self, format, Debug}, rc::Rc, sync::Arc, usize};
// use std::i32;
use log::{debug, error};

#[derive(Debug)]
pub struct Tuple {
    scheme: TupleScheme,
    fields: Vec<IntField>,
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
                    // debug!("cell bytes: {:x?}", cell_bytes);

                    let mut bytes_array = [0; 4];
                    for i in 0..4 {
                        bytes_array[i] = cell_bytes[i];
                    }
                    let value = i32::from_be_bytes(bytes_array);
                    // debug!("cell value : {}", value);

                    cells.push(IntField::new(value));

                    start = end;
                }
                Type::STRING => {}
            }
        }
        Tuple {
            scheme,
            fields: cells,
        }
    }

    pub fn new_default_tuple(scheme: TupleScheme, _width: i32) -> Tuple {
        let mut cells: Vec<IntField> = Vec::new();
        for field in &scheme.fields {
            match field.field_type {
                Type::INT => {
                    cells.push(IntField::new(0));
                }
                Type::STRING => {}
            }
        }
        Tuple {
            scheme,
            fields: cells,
        }
    }

    pub fn new_btree_tuple(value: i32, width: i32) -> Tuple {
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

    // FIXME: `impl Copy for Tuple` and get rid of this silly function.
    pub fn copy(&self) -> Tuple {
        Tuple {
            scheme: self.scheme.clone(),
            fields: self.fields.to_vec(),
        }
    }

    pub fn equal_cells(&self, expect: &Vec<i32>) -> bool {
        // for cell in &self.cells.into_iter().enumerate() {
        // // let cell_str = format!("{}, ", cell.value);
        // // content.push_str(&cell_str);
        // }
        for i in 0..self.fields.len() {
            if self.fields[i].value != expect[i] {
                error!(
                    "cell not equal, expect: {:?}, self: {:?}",
                    expect, self.fields
                );
                return false;
            }
        }
        true
    }
}

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

pub fn display_rows(rows: &Vec<Tuple>) {
    let s = format!("rows[{} in total] : [", rows.len());
    let mut content: String = s.to_owned();
    let mut slice: &[Tuple] = &Vec::new();
    if rows.len() > 5 {
        slice = &rows[..5];
    } else {
        slice = &rows[..];
    }

    for r in slice {
        let s = format!("{}, ", r);
        content.push_str(&s);
    }
    content = content[..content.len() - 2].to_string();
    content.push_str(" ... ]");
    debug!("{}", content);
}

// impl fmt::Display for Vec<Row> {
// fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
// let mut content: String = "{".to_owned();
// for cell in &self.cells {
// let cell_str = format!("{}, ", cell.value);
// content.push_str(&cell_str);
// }
// content.push_str(&"}");
// write!(f, "{}", content,)
// }
// }

#[derive(Debug)]
pub struct TupleScheme {
    fields: Vec<FieldItem>,
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
    fn new(fields: Vec<FieldItem>) -> TupleScheme {
        TupleScheme { fields: fields }
    }

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

    pub(crate) fn filedsCount(&self) -> i32 {
        self.fields.len() as i32
    }

    pub(crate) fn get_field_type(&self, i: i32) -> Type {
        self.fields[i as usize].field_type
    }

    // / get tuple size in bytes
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

pub fn simple_int_tuple_scheme(width: i32, name_prefix: &str) -> TupleScheme {
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
