use crate::bufferpool::BufferPool;
use crate::row::RowScheme;
use crate::table::*;
use log::{debug, error, info};
use std::collections::HashMap;
use std::rc::Rc;

use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use std::cell::RefCell;
use std::fs::File;
use std::sync::{Arc, Mutex, MutexGuard};

// lazy_static! {
// pub static ref db: Database = Database::new();
// }
static DB: OnceCell<Database> = OnceCell::new();

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

    pub fn global() -> &'static Database {
        DB.get_or_init(|| Database::new())
        // match DB.get() {
        // Some(db) => db,
        // None => {
        // let db = Database::new();
        // DB.set(db).unwrap_or_default();
        // //                DB.get().unwrap()
        // DB.get_or_init(|| {
        //
        // })
        // }
        // }
        // expect("db is not initialized")
    }

    pub(crate) fn get_catalog(&self) -> MutexGuard<Catalog> {
        self.catalog.lock().unwrap()
    }

    pub(crate) fn get_buffer_pool(&self) -> MutexGuard<BufferPool> {
        self.buffer_pool.lock().unwrap()
    }
}

pub struct Catalog {
    table_id_table_map: HashMap<i32, Arc<Mutex<HeapTable>>>,
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
            Some(t) => t.lock().unwrap().get_row_scheme(),
            None => panic!(""),
        }
    }

    pub(crate) fn add_table(
        &mut self,
        table: HeapTable,
        table_name: &str,
        primary_key: &str,
    ) {
        self.table_id_table_map
            .insert(table.get_id(), Arc::new(Mutex::new(table)));
    }

    pub fn get_table(&self, table_id: i32) -> MutexGuard<HeapTable> {
//        debug!("{:?}", self.table_id_table_map);
        self.table_id_table_map.get(&table_id).unwrap().lock().unwrap()
    }
}
