use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use log::debug;

use crate::{
    btree::table::NestedIterator,
    io::{Decodeable, Encodeable, SmallReader},
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
    pub fn load_schema() -> SmallResult {
        let schema_table_rc = Arc::new(RwLock::new(BTreeTable::new(
            SCHEMA_TBALE_NAME,
            0,
            &Schema::for_schema_table(),
        )));

        // add the table "schema"
        {
            let mut catalog = Database::mut_catalog();
            catalog.add_table(schema_table_rc.clone());
        }

        // scan the catalog table and load all the tables
        let mut schemas = HashMap::new();
        let mut table_names = HashMap::new();

        let tx = Transaction::new();
        tx.start()?;
        let schema_table = schema_table_rc.rl();
        let mut iter = schema_table.iter(&tx);
        while let Some(tuple) = iter.next() {
            let table_id = tuple.get_cell(0).get_int64()?;
            let table_name = tuple.get_cell(1).get_string()?;
            let field_name = tuple.get_cell(2).get_string()?;
            let field_type = tuple.get_cell(3).get_string()?;
            let is_primary = tuple.get_cell(4).get_bool()?;

            let mut fields = Vec::new();
            fields.push(Field::new(
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
                &table_name,
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

    pub fn add_table(&mut self, table: Value) {
        // let id = table.rl().get_id();
        // self.map.insert(id, Arc::clone(&table));

        let table_index: Key = 0;
        let table_rc = self.get_table(&table_index);
        let table = table_rc.unwrap().rl();

        let tx = Transaction::new();
        tx.start().unwrap();

        for (i, field) in
            table.get_tuple_scheme().fields.iter().enumerate()
        {
            let cells = vec![
                // table id
                Cell::new_int64(table.get_id() as i64),
                // table name
                Cell::new_string(&table.name),
                // field name
                Cell::new_string(&field.name),
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
}
