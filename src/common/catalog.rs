use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

use log::error;

use crate::{
    io::{Decodeable, SmallReader},
    storage::schema::{FieldItem, Schema, Type},
    transaction::Transaction,
    types::SmallResult,
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

    /// Load the catalog from disk.
    pub fn load_schema(
        &mut self,
        catalog_file_path: &str,
    ) -> SmallResult {
        let catalog_schema = Schema {
            fields: vec![
                FieldItem::new("table_id", Type::Int64, true),
                FieldItem::new("table_name", Type::Char(255), false),
                FieldItem::new("field_name", Type::Char(10), false),
                FieldItem::new("field_type", Type::Char(10), false),
                FieldItem::new("is_primary", Type::Bool, false),
            ],
        };

        let catalog_table =
            BTreeTable::new(catalog_file_path, 0, &catalog_schema);

        // scan the catalog table and load all the tables
        let mut schemas = HashMap::new();
        let mut table_names = HashMap::new();

        let tx = Transaction::new();
        tx.start()?;
        let mut iter = catalog_table.iter(&tx);
        while let Some(tuple) = iter.next() {
            let table_id = tuple.get_cell(0).get_int64()?;
            let table_name = tuple.get_cell(1).get_string()?;
            let field_name = tuple.get_cell(2).get_string()?;
            let field_type = tuple.get_cell(3).get_string()?;
            let is_primary = tuple.get_cell(4).get_bool()?;

            let mut fields = Vec::new();
            fields.push(FieldItem::new(
                &field_name,
                Type::read_from(&mut SmallReader::new(
                    field_type.as_bytes(),
                )),
                is_primary,
            ));

            schemas.insert(table_id, fields);
            table_names.insert(table_id, table_name);
        }

        for (table_id, fields) in schemas {
            let table_schema = Schema { fields };
            let table_name = table_names.get(&table_id).unwrap();

            let table_file_path = Path::new(catalog_file_path)
                .parent()
                .unwrap()
                .join(table_name);
            let table_file_path = table_file_path.to_str().unwrap();

            let mut key_field = 0;
            for (i, field) in table_schema.fields.iter().enumerate() {
                if field.is_primary {
                    key_field = i;
                    break;
                }
            }

            let table = BTreeTable::new(
                table_file_path,
                key_field,
                &table_schema,
            );
            self.add_table(Arc::new(RwLock::new(table)));
        }

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
