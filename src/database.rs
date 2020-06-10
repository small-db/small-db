use crate::row::RowScheme;
use crate::table::Table;
use crate::bufferpool::BufferPool;
use std::collections::HashMap;
use std::rc::Rc;

use lazy_static::lazy_static;
use std::sync::{Arc, Mutex, MutexGuard};
use std::cell::RefCell;
lazy_static! {
    pub static ref db: Database = Database::new();
}

pub struct Database {
    catalog: Arc<Mutex<Catalog>>,
    buffer_pool: BufferPool,
}

impl Database {
    pub(crate) fn new() -> Database {
        Database {
            catalog: Arc::new(Mutex::new(Catalog::new())),
            buffer_pool: BufferPool::new(),
        }
    }

    pub(crate) fn get_catalog(&self) -> MutexGuard<Catalog> {
//        &mut self.catalog
//        &mut *self.catalog.borrow_mut()
//        Arc::clone(&self.catalog)
        self.catalog.lock().unwrap()
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

    pub(crate) fn get_row_scheme(&self, table_id: i32) -> Arc<RowScheme> {
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
