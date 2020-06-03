use std::any::Any;
use std::collections::HashMap;

struct Row {
    scheme: RowScheme,
    cells: Vec<Box<dyn Cell>>,
}

impl Row {
    fn new(scheme: RowScheme) -> Row {
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

    fn set_cell(&mut self, i: u32, c: Box<dyn Cell>) {
        self.cells[i as usize] = c;
    }

    fn get_cell(&mut self, i: u32) -> Box<dyn Cell> {
        self.cells[i as usize].clone_box()
    }
}

#[derive(Debug)]
struct RowScheme {
    fields: Vec<FieldItem>,
}

impl PartialEq for RowScheme {
    fn eq(&self, other: &Self) -> bool {
        let matching = self.fields.iter().zip(&other.fields).filter(|&(a, b)| a == b).count();
        self.fields.len() == matching
    }
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

#[derive(PartialEq, Debug)]
struct FieldItem {
    field_type: Type,
    field_name: String,
}

trait Cell: CellClone {
    fn as_any(&self) -> &dyn Any;
}

trait CellClone {
    fn clone_box(&self) -> Box<Cell>;
}

impl<T> CellClone for T
where
    T: 'static + Cell + Clone,
{
    fn clone_box(&self) -> Box<Cell> {
        Box::new(self.clone())
    }
}

// We can now implement Clone manually by forwarding to clone_box.
impl Clone for Box<Cell> {
    fn clone(&self) -> Box<Cell> {
        self.clone_box()
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
struct IntCell {
    value: i128,
}

impl IntCell {
    fn new(v: i128) -> IntCell {
        IntCell { value: v }
    }
}

impl Cell for IntCell {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct Database {
    catalog: Catalog,
}

impl Database {
    fn new() -> Database {
        Database {
            catalog: Catalog::new(),
        }
    }

    fn get_catalog(&mut self) -> &mut Catalog {
        &mut self.catalog
    }
}

trait Table {
    fn get_row_scheme(&self) -> &RowScheme;
    fn get_id(&self) -> i32;
}

struct SkeletonTable {
    table_id: i32,
    row_scheme: RowScheme,
}

impl Table for SkeletonTable {
    fn get_row_scheme(&self) -> &RowScheme {
        &self.row_scheme
    }

    fn get_id(&self) -> i32 {
        self.table_id
    }
}

struct Catalog {
    table_id_table_map: HashMap<i32, Box<Table>>,
}

impl Catalog {
    fn new() -> Catalog {
        Catalog {
            table_id_table_map: HashMap::new(),
        }
    }

    fn get_row_scheme(&self, table_id: i32) -> &RowScheme {
        let t = self.table_id_table_map.get(&table_id);
        match t {
            Some(t) => t.get_row_scheme(),
            None => panic!(""),
        }
    }

    fn add_table(&mut self, table: Box<dyn Table>, table_name: &str, primary_key: &str) {
        self.table_id_table_map.insert(table.get_id(), table);
    }
    // let b: &B = match a.as_any().downcast_ref::<B>() {
    //     Some(b) => b,
    //     None => panic!("&a isn't a B!"),
    // }
}

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

        let mut row = Row::new(scheme);
        row.set_cell(0, Box::new(IntCell::new(-1)));
        row.set_cell(1, Box::new(IntCell::new(0)));

        assert_eq!(
            IntCell::new(-1),
            *row.get_cell(0).as_any().downcast_ref::<IntCell>().unwrap()
        );
        assert_eq!(
            IntCell::new(0),
            *row.get_cell(1).as_any().downcast_ref::<IntCell>().unwrap()
        );
    }

    #[test]
    fn get_row_scheme() {
        // setup
        let mut db = Database::new();
        let table_id_1 = 3;
        let table_id_2 = 5;
        let table_1 = SkeletonTable{
            table_id: table_id_1,
            row_scheme: simple_int_row_scheme(2, ""),
        };
        let table_2 = SkeletonTable{
            table_id: table_id_2,
            row_scheme: simple_int_row_scheme(2, ""),
        };
        db.get_catalog().add_table(Box::new(table_1), "table1", "");
        db.get_catalog().add_table(Box::new(table_2), "table2", "");
//        db.get_catalog().add_table(table_2, "table2", "");

        let expected = simple_int_row_scheme(2, "");
        let actual = db.get_catalog().get_row_scheme(table_id_1);
        assert_eq!(expected, *actual);
    }
}
