use crate::cell::{Cell, FieldItem, IntCell, Type};

pub struct Row {
    scheme: RowScheme,
    cells: Vec<Box<dyn Cell>>,
}

impl Row {
    pub fn new(scheme: RowScheme) -> Row {
        let mut cells: Vec<Box<dyn Cell>> = Vec::new();
        for field in &scheme.fields {
            match field.field_type {
                Type::INT => {
                    cells.push(Box::new(IntCell::new(0)));
                }
                Type::STRING => {}
            }
        }
        Row {
            scheme: scheme,
            cells: cells,
        }
    }

    pub fn set_cell(&mut self, i: i32, c: Box<dyn Cell>) {
        self.cells[i as usize] = c;
    }

    pub fn get_cell(&mut self, i: i32) -> Box<dyn Cell> {
        self.cells[i as usize].clone_box()
    }
}

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
