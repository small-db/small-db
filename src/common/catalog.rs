use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    field::{FieldItem, Type},
    storage::schema::Schema,
    utils::HandyRwLock,
    BTreeTable,
};

pub struct Catalog {
    map: HashMap<Key, Value>,
}

type Key = u32;
type Value = Arc<RwLock<BTreeTable>>;

impl Catalog {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn load_schema(&mut self, catalog_file_path: &str) {
        // let catalog_schema = small_int_schema(3, "catalog");

        let catalog_schema = Schema {
            fields: vec![
                FieldItem::new("table_id", Type::INT),
                // FieldItem::new("table_id", Type::INT),
                // FieldItem::new("table_name", Type::VARCHAR),
                // FieldItem::new("table_path", Type::VARCHAR),
            ],
        };

        let _catalog_table =
            BTreeTable::new(catalog_file_path, 0, &catalog_schema);

        todo!()
    }

    pub fn get_table(&self, table_index: &Key) -> Option<&Value> {
        self.map.get(table_index)
    }

    pub fn get_tuple_scheme(
        &self,
        table_index: &Key,
    ) -> Option<Schema> {
        let table_rc = self.map.get(table_index);
        match table_rc {
            Some(table_rc) => {
                let table = table_rc.rl();
                Some(table.get_tuple_scheme())
            }
            None => None,
        }
    }

    pub fn add_table(&mut self, file: Value) {
        self.map.insert(file.rl().get_id(), Arc::clone(&file));

        // TODO: write to catalog file
        todo!()
    }
}
