use std::{
    cell::RefCell,
    collections::HashMap,
    rc::{Rc, Weak},
};

use super::{
    database::Database,
    file::{BTreeLeafPage, BTreePageID},
    database_singleton::singleton_db,
};

// pub const BUFFER_POOL: HashMap<i32, BTreeLeafPage> = HashMap::new();

pub struct BufferPool {
    buffer: HashMap<Key, Value>,
}

type Key = BTreePageID;
type Value = Rc<RefCell<BTreeLeafPage>>;

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool {
            buffer: HashMap::new(),
        }
    }

    pub fn get_page(&mut self, key: &Key) -> Option<&Value> {
        let result = self.buffer.get(key);
        match result {
            Some(v) => Some(v),
            None => {
                    // get file from disk

                    // 1. get db file
                    let db = singleton_db() ;
                    let ct = db.get_catalog();
                    let table_id = key.get_table_id();
                    let f = ct.borrow().get_db_file(&table_id).unwrap();
                    todo!()
                }
        }
    }
}
