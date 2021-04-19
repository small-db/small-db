use std::{cell::RefCell, collections::HashMap, rc::Rc};

use super::{
    database::Database,
    file::{BTreeFile, BTreeLeafPage, BTreePageID},
};

pub struct Catalog {
    map: HashMap<Key, Value>,
}

type Key = i32;
type Value = Rc<RefCell<BTreeFile>>;

impl Catalog {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn get_db_file(&self, key: &Key) -> Option<&Value> {
        self.map.get(key)
    }

    pub fn add_table(&mut self, file: Value) {
        self.map.insert(file.borrow().get_id(), Rc::clone(&file));
    }
}
