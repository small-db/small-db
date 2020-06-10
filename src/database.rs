use crate::row::RowScheme;
use crate::table::Table;
use crate::bufferpool::BufferPool;
use std::collections::HashMap;
use std::rc::Rc;

use lazy_static::lazy_static;
use std::sync::Arc;
lazy_static! {
    pub static ref db: Database = Database::new();
}

pub struct Database {
    catalog: Catalog,
    buffer_pool: BufferPool,
}

impl Database {
    pub(crate) fn new() -> Database {
        Database {
            catalog: Catalog::new(),
            buffer_pool: BufferPool::new(),
        }
    }

    pub(crate) fn get_catalog(&mut self) -> &mut Catalog {
        &mut self.catalog
    }

    pub(crate) fn get_buffer_pool(&mut self) -> &mut BufferPool {
        &mut self.buffer_pool
    }
}

pub struct Catalog {
    table_id_table_map: HashMap<i32, Arc<dyn Table>>,
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

    pub(crate) fn add_table(&mut self, table: Arc<dyn Table>, table_name: &str, primary_key: &str) {
        self.table_id_table_map
            .insert(table.get_id(), Arc::clone(&table));
    }
}
