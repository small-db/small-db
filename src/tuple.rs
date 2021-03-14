use crate::cell::*;
use std::{cell::RefCell, fmt, rc::Rc, sync::Arc};
// use std::i32;
use log::{debug, error};

#[derive(Debug)]
pub struct Tuple {
    scheme: Arc<TupleScheme>,
    cells: Vec<IntCell>,
}

impl Tuple {
    pub fn new(scheme: Arc<TupleScheme>, bytes: &[u8]) -> Tuple {
        let mut cells: Vec<IntCell> = Vec::new();
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

                    cells.push(IntCell::new(value));

                    start = end;
                }
                Type::STRING => {}
            }
        }
        Tuple {
            scheme: Arc::clone(&scheme),
            cells: cells,
        }
    }

    pub fn new_default_tuple(scheme: Arc<TupleScheme>, width: i32) -> Tuple {
        let mut cells: Vec<IntCell> = Vec::new();
        for field in &scheme.fields {
            match field.field_type {
                Type::INT => {
                    cells.push(IntCell::new(0));
                }
                Type::STRING => {}
            }
        }
        Tuple {
            scheme: Arc::clone(&scheme),
            cells,
        }
    }

    pub fn new_btree_tuple(n: i32, width: i32) -> Tuple {
        let scheme = simple_int_tuple_scheme(width, "");
        let bytes = [0];
        let mut tuple = Tuple::new_default_tuple(Arc::new(scheme), width);
        for i in 0..tuple.cells.len() {
            tuple.set_cell(i as i32, IntCell::new(n));
        }
        tuple
    }

    pub fn set_cell(&mut self, i: i32, c: IntCell) {
        self.cells[i as usize] = c;
    }

    pub fn get_cell(&mut self, i: i32) -> IntCell {
        self.cells[i as usize]
    }

    // FIXME: `impl Copy for Row` and get rid of this silly function.
    pub fn copy_row(&self) -> Tuple {
        Tuple {
            scheme: Arc::clone(&self.scheme),
            cells: self.cells.to_vec(),
        }
    }

    pub fn equal_cells(&self, expect: &Vec<i32>) -> bool {
        // for cell in &self.cells.into_iter().enumerate() {
        // // let cell_str = format!("{}, ", cell.value);
        // // content.push_str(&cell_str);
        // }
        for i in 0..self.cells.len() {
            if self.cells[i].value != expect[i] {
                error!(
                    "cell not equal, expect: {:?}, self: {:?}",
                    expect, self.cells
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
        for cell in &self.cells {
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

    pub fn get_size(&self) -> usize {
        self.fields.len() * 4
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