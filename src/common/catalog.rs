use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, RwLock},
};

use log::debug;

use crate::{
    btree::table::{BTreeTableSearchIterator, NestedIterator},
    io::{read_into, Decodeable, Encodeable},
    storage::{
        schema::{Field, Schema, Type},
        tuple::{Cell, Tuple},
    },
    transaction::Transaction,
    types::SmallResult,
    utils::HandyRwLock,
    BTreeTable, Database, Op, Predicate,
};

const SCHEMA_TBALE_NAME: &str = "schemas";
const SCHEMA_TBALE_ID: u32 = 123;

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
    ///
    /// TODO: remove this api
    pub fn load_schemas() -> SmallResult {
        let schema_table_rc =
            Database::mut_catalog().get_schema_table();
        // .get_table(&SCHEMA_TBALE_ID)
        // .unwrap();

        // add the system-table "schema"
        Catalog::add_table(schema_table_rc.clone(), false);

        // return Ok(());

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
            let schema = Schema::new(fields);
            let table_name = table_names.get(&table_id).unwrap();

            let table = BTreeTable::new(
                &table_name,
                Some(table_id as u32),
                &schema,
            );

            Catalog::add_table(Arc::new(RwLock::new(table)), false);
        }

        let catalog = Database::catalog();
        debug!("catalog: {:?}", catalog.map.keys());

        tx.commit().unwrap();

        Ok(())
    }

    /// Get the table from the catalog.
    ///
    /// If the table is not in the cached map of the catalog, then
    /// search it in the `schemas` table and load it into the map.
    ///
    /// Return the table if it exists, otherwise return `None`.
    pub fn get_table(&mut self, table_index: &Key) -> Option<Value> {
        if let Some(table_rc) = self.map.get(table_index) {
            return Some(table_rc.clone());
        }

        let schema_table_rc = self.get_schema_table();
        let schema_table = schema_table_rc.rl();

        let tx = Transaction::new();
        tx.start().unwrap();

        let predicate = Predicate::new(
            Op::Equals,
            &Cell::Int64(*table_index as i64),
        );
        let iter = BTreeTableSearchIterator::new(
            &tx,
            &schema_table,
            &predicate,
        );
        let mut fields = Vec::new();
        let mut table_name_option: Option<String> = None;
        for tuple in iter {
            table_name_option = Some(read_into(&mut Cursor::new(
                tuple.get_cell(1).get_bytes().unwrap(),
            )));

            let field_name: String = read_into(&mut Cursor::new(
                tuple.get_cell(2).get_bytes().unwrap(),
            ));
            let field_type = read_into(&mut Cursor::new(
                tuple.get_cell(3).get_bytes().unwrap(),
            ));
            let is_primary = tuple.get_cell(4).get_bool().unwrap();

            let field =
                Field::new(&field_name, field_type, is_primary);
            fields.push(field);
        }

        match table_name_option {
            Some(table_name) => {
                let schema = Schema::new(fields);
                let table = BTreeTable::new(
                    &table_name,
                    Some(*table_index),
                    &schema,
                );

                let table_rc = Arc::new(RwLock::new(table));

                self.map.insert(*table_index, table_rc.clone());
                Some(table_rc)
            }
            None => {
                return None;
            }
        }
    }

    pub fn get_schema_table(&mut self) -> Value {
        self.map
            .entry(SCHEMA_TBALE_ID)
            .or_insert_with(|| {
                let schema_table_rc =
                    Arc::new(RwLock::new(BTreeTable::new(
                        SCHEMA_TBALE_NAME,
                        Some(SCHEMA_TBALE_ID),
                        &Schema::for_schema_table(),
                    )));
                schema_table_rc
            })
            .clone()
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

        for field in table.get_schema().get_fields() {
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
