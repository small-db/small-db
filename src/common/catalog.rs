use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use log::error;

use crate::{
    btree::table::NestedIterator,
    common::database,
    io::{Decodeable, SmallReader},
    storage::schema::{FieldItem, Schema, Type},
    transaction::Transaction,
    types::SmallResult,
    utils::HandyRwLock,
    BTreeTable, Database,
};

pub struct Catalog {
    map: HashMap<Key, Value>,
}

type Key = u32;
type Value = Arc<RwLock<BTreeTable>>;

impl Catalog {
    pub fn new() -> Self {
        let mut map = HashMap::new();

        // add the table "schema"
        let catalog_table = BTreeTable::new(
            Database::global().path_schema_table(),
            0,
            &Schema::for_schema_table(),
        );

        Self { map }
    }

    /// Load the catalog from disk.
    pub fn load_schema() -> SmallResult {
        let schema_table_path =
            &Database::global().path_schema_table();

        let table_fields = Schema::for_schema_table();

        let schema_table =
            BTreeTable::new(schema_table_path, 0, &table_fields);

        // scan the catalog table and load all the tables
        let mut schemas = HashMap::new();
        let mut table_names = HashMap::new();

        let tx = Transaction::new();
        tx.start()?;
        let mut iter = schema_table.iter(&tx);

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

            let mut key_field = 0;
            for (i, field) in table_schema.fields.iter().enumerate() {
                if field.is_primary {
                    key_field = i;
                    break;
                }
            }

            let table = BTreeTable::new(
                &Database::global().path_schema_table(),
                key_field,
                &table_schema,
            );

            {
                let mut catalog = Database::mut_catalog();
                catalog.add_table(Arc::new(RwLock::new(table)));
            }
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
