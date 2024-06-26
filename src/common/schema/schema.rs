use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use crate::{BTreeTable, Database};

pub struct Schema {
    pub id: u32,

    pub name: String,
}

impl Schema {
    pub fn new(name: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);

        let unix_time = SystemTime::now();
        unix_time.hash(&mut hasher);

        let id = hasher.finish() as u32;

        Self {
            id,
            name: name.to_string(),
        }
    }

    pub fn search_table(&self, table_name: &str) -> Option<Arc<RwLock<BTreeTable>>> {
        return Database::catalog().search_table(table_name);
    }
}
