struct Row<'a> {
    scheme: &'a RowScheme,
    // cells: Vec<&'a dyn Cell>,
    cells: Vec<Box<dyn Cell>>,
}

impl<'a> Row<'a> {
    fn new(scheme: &RowScheme) -> Row  {
        // let mut cells: Vec<&'a Cell> = Vec::new();
        let mut cells: Vec<Box<dyn Cell>> = Vec::new();
        // for i in 0..scheme.filedsCount() {
        for field in &scheme.fields {
            match field.field_type {
                Type::INT => {
                    // cells.push(IntCell::new(0));
                    cells.push(Box::new(IntCell::new(0)));
                }
                Type::STRING => {
                    // cells.push(Box::new(Strin::new(0)));
                }
            }
        }
            // cells.push(Box::new(dyn Cell));
        // row.set_cell(0, Box::new(IntCell::new(-1)));
        // }
        Row {
            scheme: scheme,
            cells: Vec::new(),
        }
    }

    fn set_cell(&'a mut self, i: u32, c: impl Cell) {
        let new_c = Box::new(c.clone());
        self.cells.push(new_c);
        // use std::mem;
        // mem::replace(&mut self.cells[i as usize], None)
    }
}

// impl Default for Row {
//     fn default() -> Row {
//         Row { fields: Vec::new() }
//     }
// }

struct RowScheme {
    fields: Vec<FieldItem>,
}

impl RowScheme {
    fn new(fields: Vec<FieldItem>) -> RowScheme {
        RowScheme { fields: fields }
    }

    fn merge(scheme1: RowScheme, scheme2: RowScheme) -> RowScheme {
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

    fn filedsCount(&self) -> u32 {
        self.fields.len() as u32
    }

    fn get_field_type(&self, i: u32) -> Type {
        self.fields[i as usize].field_type
    }
}

impl Default for RowScheme {
    fn default() -> RowScheme {
        RowScheme { fields: Vec::new() }
    }
}

fn simple_int_row_scheme(number: u32, name_prefix: &str) -> RowScheme {
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

#[derive(Copy, Clone, PartialEq, Debug)]
enum Type {
    INT,
    STRING,
}

struct FieldItem {
    field_type: Type,
    field_name: String,
}

trait Cell {
    // fn new() -> Cell;
    // fn copy(&self) -> Cell where Self: Sized;
    fn new_clone(&self);
}

// #[derive(Copy, Clone, PartialEq, Debug)]
struct IntCell {
    value: i128,
}

impl IntCell {
    fn new(v: i128) -> IntCell {
        IntCell{value: v}
    }
}

impl Cell for IntCell {
    fn new_clone(&self){
        // self.clone()
    }
}
// impl Copy for IntCell {}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn combine() {
        let scheme1 = simple_int_row_scheme(1, "scheme1");
        let scheme2 = simple_int_row_scheme(2, "scheme1");

        let scheme3 = RowScheme::merge(scheme1, scheme2);

        assert_eq!(scheme3.filedsCount(), 3);
    }

    #[test]
    fn get_field_type() {
        let lengths = vec![1, 2, 1000];

        for l in lengths {
            let scheme = simple_int_row_scheme(l, "");
            for i in 0..l {
                assert_eq!(Type::INT, scheme.get_field_type(i));
            }
        }
    }

    #[test]
    fn modify_fields() {
        let scheme = simple_int_row_scheme(2, "");

        let mut row = Row::new(&scheme);
        row.set_cell(0, Box::new(IntCell::new(-1)));
    }
}
