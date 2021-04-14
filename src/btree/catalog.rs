use std::{
    cell::RefCell,
    collections::HashMap,
    rc::{Rc, Weak},
};

use super::{database::Database, file::{BTreeFile, BTreeLeafPage, BTreePageID}};

pub struct Catalog {
    map: HashMap<Key, Value>,

    db: Weak<Database>,
}

type Key = i32;
type Value = Rc<RefCell<BTreeFile>>;

impl Catalog {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            db: Weak::new(),
        }
    }

    pub fn set_db(&mut self, db: Rc<Database>) {
        self.db = Rc::downgrade(&db);
    }

    pub fn get_db_file(&self, key: &Key) -> Option<&Value> {
        self.map.get(key)
    }
}