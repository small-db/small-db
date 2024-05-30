use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, RwLock},
};

use log::info;

use super::schema::Schema;
use crate::{
    btree::table::BTreeTableSearchIterator,
    io::{read_into, Decodeable, Encodeable},
    storage::{
        table_schema::{Field, TableSchema, Type},
        tuple::{Cell, Tuple},
    },
    transaction::Transaction,
    types::SmallResult,
    utils::HandyRwLock,
    BTreeTable, Database, Op, Predicate,
};

// The namd and id for the table "tables"
const TABLE_SCHEMA_NAME: &str = "tables";
const TABLE_SCHEMA_ID: u32 = 123;

// The namd and id for the table "schemas"
const SCHEMA_NAME: &str = "schemas";
const SCHEMA_ID: u32 = 124;

type TableID = u32;
type TableRC = Arc<RwLock<BTreeTable>>;

type SchemaID = u32;
type SchemaRC = Arc<RwLock<Schema>>;

pub struct Catalog {
    tables: HashMap<TableID, TableRC>,

    schemas: HashMap<SchemaID, SchemaRC>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),

            schemas: HashMap::new(),
        }
    }

    /// Load tables from disk.
    ///
    /// We cannot pass the `self` reference to this function, since "catalog" is
    /// used inside the table write/read api, and it will cause permanent
    /// blocking.
    pub fn load_tables() -> SmallResult {
        let tables = Database::mut_catalog().get_table_schemas();

        // Add the system-table "schema", otherwise we cannot load the tables
        // from disk.
        //
        // All "add_table" calls in this function should not persist the table,
        // because we are loading the tables from disk.
        Catalog::add_table(tables.clone(), false);

        // scan the catalog table and load all the tables
        let mut schemas = HashMap::new();
        let mut table_names = HashMap::new();

        let tx = Transaction::new();
        let schema_table = tables.rl();
        let mut iter = schema_table.iter(&tx);
        while let Some(tuple) = iter.next() {
            let table_id = tuple.get_cell(0).get_int64()?;
            let table_name = String::from_utf8(tuple.get_cell(1).get_bytes()?).unwrap();
            let field_name = String::from_utf8(tuple.get_cell(2).get_bytes()?).unwrap();
            let field_type = Type::decode_from(&mut Cursor::new(tuple.get_cell(3).get_bytes()?));
            let is_primary = tuple.get_cell(4).get_bool()?;

            let field = Field::new(&field_name, field_type, is_primary);

            // insert the field into the schema, if "table_id" is not
            // in the map, then insert a new vector
            schemas.entry(table_id).or_insert_with(Vec::new).push(field);
            table_names.insert(table_id, table_name);
        }

        for (table_id, fields) in schemas {
            let schema = TableSchema::new(fields);
            let table_name = table_names.get(&table_id).unwrap();

            let table = BTreeTable::new(&table_name, Some(table_id as u32), &schema);

            // All "add_table" calls in this function should not persist the table,
            // because we are loading the tables from disk.
            Catalog::add_table(Arc::new(RwLock::new(table)), false);
        }

        tx.commit().unwrap();

        {
            // Insert table "pg_database" if it does not exist.
            let mut catalog = Database::mut_catalog();
            catalog.search_table("pg_database").unwrap_or_else(|| {
                let schema = TableSchema::for_pg_database();
                let table = BTreeTable::new("pg_database", Some(0), &schema);
                let table_rc = Arc::new(RwLock::new(table));
                catalog.add_table_to_memory(table_rc.clone());
                table_rc
            });
        }

        Ok(())
    }

    /// Load tables from disk.
    ///
    /// We cannot pass the `self` reference to this function, since "catalog" is
    /// used inside the table write/read api, and it will cause permanent
    /// blocking.
    pub fn load_schemas() -> SmallResult {
        let schemas_rc = Database::mut_catalog().get_schemas();

        let tx = Transaction::new();
        let schemas = schemas_rc.rl();
        let iter = schemas.iter(&tx);
        for v in iter {
            info!("schema: {:?}", v);
        }

        {
            // Insert schema "pg_catalog" if it does not exist.
            let mut catalog = Database::mut_catalog();
            catalog.search_schema("pg_catalog").unwrap_or_else(|| {
                let schema = Schema::new("pg_catalog");
                let schema_rc = Arc::new(RwLock::new(schema));
                catalog.schemas.insert(0, schema_rc.clone());
                schema_rc
            });
        }

        Ok(())
    }

    /// Get the table from the catalog.
    ///
    /// If the table is not in the cached map of the catalog, then
    /// search it in the `schemas` table and load it into the map.
    ///
    /// Return the table if it exists, otherwise return `None`.
    pub fn get_table(&mut self, table_index: &TableID) -> Option<TableRC> {
        if let Some(table_rc) = self.tables.get(table_index) {
            return Some(table_rc.clone());
        }

        let schema_table_rc = self.get_table_schemas();
        let schema_table = schema_table_rc.rl();

        let tx = Transaction::new();

        let predicate = Predicate::new(
            schema_table.key_field,
            Op::Equals,
            &Cell::Int64(*table_index as i64),
        );
        let iter = BTreeTableSearchIterator::new(&tx, &schema_table, &predicate);
        let mut fields = Vec::new();
        let mut table_name_option: Option<String> = None;
        for tuple in iter {
            table_name_option = Some(read_into(&mut Cursor::new(
                tuple.get_cell(1).get_bytes().unwrap(),
            )));

            let field_name: String =
                read_into(&mut Cursor::new(tuple.get_cell(2).get_bytes().unwrap()));
            let field_type = read_into(&mut Cursor::new(tuple.get_cell(3).get_bytes().unwrap()));
            let is_primary = tuple.get_cell(4).get_bool().unwrap();

            let field = Field::new(&field_name, field_type, is_primary);
            fields.push(field);
        }

        match table_name_option {
            Some(table_name) => {
                let schema = TableSchema::new(fields);
                let table = BTreeTable::new(&table_name, Some(*table_index), &schema);

                let table_rc = Arc::new(RwLock::new(table));

                self.tables.insert(*table_index, table_rc.clone());
                Some(table_rc)
            }
            None => {
                return None;
            }
        }
    }

    /// TODO: remove this old api
    pub fn search_table_old(table_name: &str) -> Option<TableRC> {
        let schema_table_rc = Database::mut_catalog().get_table_schemas();
        let schema_table = schema_table_rc.rl();

        let tx = Transaction::new();

        // TODO: get index in a stable way
        let table_name_index = schema_table.get_schema().get_field_pos("table_name");

        let predicate = Predicate::new(
            table_name_index,
            Op::Equals,
            &Cell::Bytes(table_name.as_bytes().to_vec()),
        );
        let iter = BTreeTableSearchIterator::new(&tx, &schema_table, &predicate);
        let mut fields = Vec::new();
        let mut table_id_option: Option<i64> = None;
        for tuple in iter {
            table_id_option = Some(tuple.get_cell(0).get_int64().unwrap());

            let field_name = String::from_utf8(tuple.get_cell(2).get_bytes().unwrap()).unwrap();
            let field_type = read_into(&mut Cursor::new(tuple.get_cell(3).get_bytes().unwrap()));
            let is_primary = tuple.get_cell(4).get_bool().unwrap();

            let field = Field::new(&field_name, field_type, is_primary);
            fields.push(field);
        }

        tx.commit().unwrap();

        match table_id_option {
            Some(table_id) => {
                let schema = TableSchema::new(fields);
                let table = BTreeTable::new(table_name, Some(table_id as u32), &schema);

                let table_rc = Arc::new(RwLock::new(table));

                Database::mut_catalog()
                    .tables
                    .insert(table_id as u32, table_rc.clone());
                Some(table_rc)
            }
            None => {
                return None;
            }
        }
    }

    pub fn get_table_schemas(&mut self) -> TableRC {
        self.tables
            .entry(TABLE_SCHEMA_ID)
            .or_insert_with(|| {
                let table_rc = Arc::new(RwLock::new(BTreeTable::new(
                    TABLE_SCHEMA_NAME,
                    Some(TABLE_SCHEMA_ID),
                    &TableSchema::for_table_schema(),
                )));
                table_rc
            })
            .clone()
    }

    pub fn get_schemas(&mut self) -> TableRC {
        self.tables
            .entry(SCHEMA_ID)
            .or_insert_with(|| {
                let table_rc = Arc::new(RwLock::new(BTreeTable::new(
                    SCHEMA_NAME,
                    Some(SCHEMA_ID),
                    &&TableSchema::for_schemas(),
                )));
                table_rc
            })
            .clone()
    }

    fn add_table_to_memory(&mut self, table_rc: TableRC) {
        let id = table_rc.rl().get_id();
        self.tables.insert(id, Arc::clone(&table_rc));
    }

    fn add_table_to_disk(table_rc: TableRC) {
        let table = table_rc.rl();

        let schema_table_rc = Database::mut_catalog().get_table_schemas();
        let schema_table = schema_table_rc.rl();

        let tx = Transaction::new();

        let schema_fields = schema_table.schema.get_fields();
        let table_name_type = schema_fields[1].get_type();
        let field_name_type = schema_fields[2].get_type();
        let field_type_type = schema_fields[3].get_type();

        for field in table.get_schema().get_fields() {
            // let t = field.get_type();
            let cells = vec![
                // table id
                Cell::new_int64(table.get_id() as i64),
                // table name
                Cell::new_bytes(&table.name.as_bytes(), &table_name_type),
                // field name
                Cell::new_bytes(&field.name.as_bytes(), &field_name_type),
                // field type
                Cell::new_bytes(&field.get_type().to_bytes(), &field_type_type),
                // is primary
                Cell::new_bool(field.is_primary),
            ];
            let tuple = Tuple::new(&cells);
            schema_table.insert_tuple(&tx, &tuple).unwrap();
        }

        tx.commit().unwrap();
    }

    pub fn add_table(table_rc: TableRC, persist: bool) {
        {
            let mut catalog = Database::mut_catalog();
            catalog.add_table_to_memory(table_rc.clone());
        }

        if persist {
            Self::add_table_to_disk(table_rc);
        }
    }

    pub fn search_schema(&self, schema_name: &str) -> Option<SchemaRC> {
        for schema_rc in self.schemas.values() {
            let schema = schema_rc.rl();
            if schema.name == schema_name {
                return Some(schema_rc.clone());
            }
        }

        None
    }

    pub fn search_table(&self, table_name: &str) -> Option<TableRC> {
        for table_rc in self.tables.values() {
            let table = table_rc.rl();
            if table.name == table_name {
                return Some(table_rc.clone());
            }
        }

        None
    }
}
