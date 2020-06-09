use crate::row::RowScheme;
use crate::table::Table;
use std::collections::HashMap;
use std::rc::Rc;

pub struct Database {
    catalog: Catalog,
}

impl Database {
    pub(crate) fn new() -> Database {
        Database {
            catalog: Catalog::new(),
        }
    }

    pub(crate) fn get_catalog(&mut self) -> &mut Catalog {
        &mut self.catalog
    }
}

pub struct Catalog {
    //    1: box<trail>

    //    2: option<box<trail>
    //    table_id_table_map: HashMap<i32, Option<Box<dyn Table>>>,

    //    3: Rc
    table_id_table_map: HashMap<i32, Rc<dyn Table>>,
}

impl Catalog {
    fn new() -> Catalog {
        Catalog {
            table_id_table_map: HashMap::new(),
        }
    }

    pub(crate) fn get_row_scheme(&self, table_id: i32) -> &RowScheme {
        let t = self.table_id_table_map.get(&table_id);
        match t {
            Some(t) => t.get_row_scheme(),
            None => panic!(""),
        }
    }

    pub(crate) fn add_table(&mut self, table: Rc<dyn Table>, table_name: &str, primary_key: &str) {
        self.table_id_table_map
            .insert(table.get_id(), Rc::clone(&table));
    }
}
