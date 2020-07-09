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
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

// lazy_static! {
// pub static ref db: Database = Database::new();
// }
static DB: OnceCell<Database> = OnceCell::new();

pub static PAGE_SIZE: usize = 4096;

pub struct Database {
    catalog: Arc<RwLock<Catalog>>,
    buffer_pool: Arc<RwLock<BufferPool>>,
}

impl Database {
    pub(crate) fn new() -> Database {
        Database {
            catalog: Arc::new(RwLock::new(Catalog::new())),
            buffer_pool: Arc::new(RwLock::new(BufferPool::new())),
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

    pub(crate) fn get_catalog(&self) -> RwLockReadGuard<Catalog> {
        // debug!("read catalog");
        self.catalog.try_read().unwrap()
    }

    pub(crate) fn get_buffer_pool(&self) -> RwLockWriteGuard<BufferPool> {
        // debug!("read buffer pool");
        self.buffer_pool.try_write().unwrap()
    }

    pub(crate) fn get_write_catalog(&self) -> RwLockWriteGuard<Catalog> {
        // debug!("write catalog");
        self.catalog.try_write().unwrap()
    }

    pub(crate) fn get_write_buffer_pool(&self) -> RwLockWriteGuard<BufferPool> {
        // debug!("write buffer pool");
        self.buffer_pool.try_write().unwrap()
    }

    pub fn add_table(table: Arc<RwLock<HeapTable>>, table_name: &str, primary_key: &str) {
        // add table to catolog
        // add a scope to release write lock (release lock at function return)
        let mut catlog = Database::global().get_write_catalog();
        catlog.add_table(Arc::clone(&table), "table", "");
    }
}

pub struct Catalog {
    table_id_table_map: HashMap<i32, Arc<RwLock<HeapTable>>>,
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
            Some(t) => t.try_read().unwrap().get_row_scheme(),
            None => panic!(""),
        }
    }

    pub(crate) fn add_table(
        &mut self,
        table: Arc<RwLock<HeapTable>>,
        table_name: &str,
        primary_key: &str,
    ) {
        self.table_id_table_map
            .insert(table.try_read().unwrap().table_id, Arc::clone(&table));
    }

    pub fn get_table(&self, table_id: i32) -> RwLockWriteGuard<HeapTable> {
        // debug!("{:?}", self.table_id_table_map);
        self.table_id_table_map
            .get(&table_id)
            .unwrap()
            .try_write()
            .unwrap()
    }

    pub fn get_write_table(&self, table_id: i32) -> RwLockWriteGuard<HeapTable> {
        // debug!("{:?}", self.table_id_table_map);
        self.table_id_table_map
            .get(&table_id)
            .unwrap()
            .try_write()
            .unwrap()
    }
}
