use crate::row::RowScheme;
use crate::table::Table;
use crate::bufferpool::BufferPool;
use std::collections::HashMap;
use std::rc::Rc;
use log::{debug, error, info};

use lazy_static::lazy_static;
use std::sync::{Arc, Mutex, MutexGuard};
use std::cell::RefCell;
use std::fs::File;

lazy_static! {
    pub static ref db: Database = Database::new();
}

pub struct Database {
    catalog: Arc<Mutex<Catalog>>,
    buffer_pool: Arc<Mutex<BufferPool>>,
}

impl Database {
    pub(crate) fn new() -> Database {
        Database {
            catalog: Arc::new(Mutex::new(Catalog::new())),
            buffer_pool: Arc::new(Mutex::new(BufferPool::new())),
        }
    }

    pub(crate) fn get_catalog(&self) -> MutexGuard<Catalog> {
        self.catalog.lock().unwrap()
    }

    pub(crate) fn get_buffer_pool(&self) -> MutexGuard<BufferPool> {
        self.buffer_pool.lock().unwrap()
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

    pub fn get_table(&self, table_id: i32) -> Arc<dyn Table> {
        debug!("{:?}", self.table_id_table_map);
        Arc::clone(self.table_id_table_map.get(&table_id).unwrap())
    }
}
