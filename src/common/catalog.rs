use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

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
    pub fn load_schemas() -> SmallResult {
        let schema_table_rc = Arc::new(RwLock::new(BTreeTable::new(
            SCHEMA_TBALE_NAME,
            0,
            &Schema::for_schema_table(),
        )));

        // add the system-table "schema"
        Catalog::add_table(schema_table_rc.clone());

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
                String::from_bytes(tuple.get_cell(1).get_bytes()?);
            let field_name =
                String::from_bytes(tuple.get_cell(2).get_bytes()?);
            let field_type =
                Type::from_bytes(tuple.get_cell(3).get_bytes()?);
            let is_primary = tuple.get_cell(4).get_bool()?;

            let mut fields = Vec::new();
            fields.push(Field::new(
                &field_name,
                field_type,
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
                &table_name,
                key_field,
                &table_schema,
            );

            Catalog::add_table(Arc::new(RwLock::new(table)));
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

    fn add_table_to_memory(&mut self, table_rc: Value) {
        let id = table_rc.rl().get_id();
        self.map.insert(id, Arc::clone(&table_rc));
    }

    fn add_table_to_disk(table_rc: Value) {
        let table = table_rc.rl();

        let tx = Transaction::new();
        tx.start().unwrap();

        for (_i, field) in
            table.get_tuple_scheme().fields.iter().enumerate()
        {
            let cells = vec![
                // table id
                Cell::new_int64(table.get_id() as i64),
                // table name
                Cell::new_bytes(&table.name),
                // field name
                Cell::new_bytes(&field.name),
                // field type
                Cell::new_bytes(&field.t.to_bytes()),
                // is primary
                Cell::new_bool(field.is_primary),
            ];
            let tuple = Tuple::new_from_cells(&cells);
            table.insert_tuple(&tx, &tuple).unwrap();
        }

        tx.commit().unwrap();
    }

    pub fn add_table(table_rc: Value) {
        {
            let mut catalog = Database::mut_catalog();
            catalog.add_table_to_memory(table_rc.clone());
        }

        Self::add_table_to_disk(table_rc);
    }
}
