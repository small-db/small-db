use crate::field::*;
use std::{
    cell::RefCell,
    fmt,
    rc::Rc,
    sync::Arc,
};
// use std::i32;
use log::{
    debug,
    error,
};

#[derive(Debug)]
pub struct Row {
    scheme: Arc<RowScheme>,
    cells: Vec<IntField>,
}

impl Row {
    pub fn new(scheme: Arc<RowScheme>, bytes: &[u8]) -> Row {
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
        Row {
            scheme: Arc::clone(&scheme),
            cells: cells,
        }
    }

    pub fn set_cell(&mut self, i: i32, c: IntField) {
        self.cells[i as usize] = c;
    }

    pub fn get_cell(&mut self, i: i32) -> IntField {
        self.cells[i as usize]
    }

    // FIXME: `impl Copy for Row` and get rid of this silly function.
    pub fn copy_row(&self) -> Row {
        Row {
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

impl fmt::Display for Row {
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

pub fn display_rows(rows: &Vec<Row>) {
    let s = format!("rows[{} in total] : [", rows.len());
    let mut content: String = s.to_owned();
    let mut slice: &[Row] = &Vec::new();
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
pub struct RowScheme {
    fields: Vec<FieldItem>,
}

impl PartialEq for RowScheme {
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

impl RowScheme {
    fn new(fields: Vec<FieldItem>) -> RowScheme {
        RowScheme { fields: fields }
    }

    pub fn merge(scheme1: RowScheme, scheme2: RowScheme) -> RowScheme {
        let mut new_scheme = RowScheme {
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

impl Default for RowScheme {
    fn default() -> RowScheme {
        RowScheme { fields: Vec::new() }
    }
}

pub fn simple_int_row_scheme(number: i32, name_prefix: &str) -> RowScheme {
    let mut fields: Vec<FieldItem> = Vec::new();
    for i in 0..number {
        let field = FieldItem {
            field_name: format!("{}-{}", name_prefix, i),
            field_type: Type::INT,
        };
        fields.push(field);
    }

    RowScheme { fields: fields }
}
