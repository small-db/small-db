use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, RwLock},
};

use log::debug;

use crate::{
    btree::table::NestedIterator,
    io::{Decodeable, Encodeable},
    storage::{
        schema::{Field, Schema, Type},
        tuple::{Cell, Tuple},
    },
    transaction::Transaction,
    types::SmallResult,
    utils::HandyRwLock,
    BTreeTable, Database,
};

const SCHEMA_TBALE_NAME: &str = "schemas";

pub struct Catalog {
    map: HashMap<Key, Value>,
    schema_table: Option<Arc<RwLock<BTreeTable>>>,
}

type Key = u32;
type Value = Arc<RwLock<BTreeTable>>;

impl Catalog {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),

            // Lazy initialization it in `load_schemas()`, since the
            // construction of `Table` relies on `Database` instance,
            // which is not initialized yet.
            schema_table: None,
        }
    }

    /// Load the catalog from disk.
    pub fn load_schemas() -> SmallResult {
        let schema_table_rc =
            Database::mut_catalog().get_schema_table();

        // add the system-table "schema"
        Catalog::add_table(schema_table_rc.clone(), false);

        // scan the catalog table and load all the tables
        let mut schemas = HashMap::new();
        let mut table_names = HashMap::new();

        let tx = Transaction::new();
        tx.start()?;
        let schema_table = schema_table_rc.rl();
        let mut iter = schema_table.iter(&tx);
        while let Some(tuple) = iter.next() {
            let table_id = tuple.get_cell(0).get_int64()?;
            let table_name =
                String::from_utf8(tuple.get_cell(1).get_bytes()?)
                    .unwrap();
            let field_name =
                String::from_utf8(tuple.get_cell(2).get_bytes()?)
                    .unwrap();
            let field_type = Type::decode_from(&mut Cursor::new(
                tuple.get_cell(3).get_bytes()?,
            ));
            let is_primary = tuple.get_cell(4).get_bool()?;

            let field =
                Field::new(&field_name, field_type, is_primary);

            // insert the field into the schema, if "table_id" is not
            // in the map, then insert a new vector
            schemas
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push(field);
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
                &table_name,
                Some(table_id as u32),
                key_field,
                &table_schema,
            );

            Catalog::add_table(Arc::new(RwLock::new(table)), false);
        }

        let catalog = Database::catalog();
        debug!("catalog: {:?}", catalog.map.keys());

        tx.commit().unwrap();

        Ok(())
    }

    pub fn get_table(&self, table_index: &Key) -> Option<&Value> {
        self.map.get(table_index)
    }

    pub fn get_schema_table(&mut self) -> Value {
        let schema_table_rc;

        match &self.schema_table {
            Some(rc) => rc.clone(),
            None => {
                schema_table_rc =
                    Arc::new(RwLock::new(BTreeTable::new(
                        SCHEMA_TBALE_NAME,
                        Some(123),
                        0,
                        &Schema::for_schema_table(),
                    )));
                self.schema_table = Some(schema_table_rc.clone());
                schema_table_rc
            }
        }
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

    fn add_table_to_memory(&mut self, table_rc: Value) {
        let id = table_rc.rl().get_id();
        self.map.insert(id, Arc::clone(&table_rc));
    }

    fn add_table_to_disk(table_rc: Value) {
        let table = table_rc.rl();

        let schema_table_rc =
            Database::mut_catalog().get_schema_table();
        let schema_table = schema_table_rc.rl();

        let tx = Transaction::new();
        tx.start().unwrap();

        for field in table.get_tuple_scheme().fields {
            let cells = vec![
                // table id
                Cell::new_int64(table.get_id() as i64),
                // table name
                Cell::new_bytes(&table.name.as_bytes()),
                // field name
                Cell::new_bytes(&field.name.as_bytes()),
                // field type
                Cell::new_bytes(&field.t.encode()),
                // is primary
                Cell::new_bool(field.is_primary),
            ];
            let tuple = Tuple::new_from_cells(&cells);
            schema_table.insert_tuple(&tx, &tuple).unwrap();
        }

        tx.commit().unwrap();
    }

    pub fn add_table(table_rc: Value, persist: bool) {
        {
            let mut catalog = Database::mut_catalog();
            catalog.add_table_to_memory(table_rc.clone());
        }

        if persist {
            Self::add_table_to_disk(table_rc);
        }
    }
}
