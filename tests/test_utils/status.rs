use std::{
    fs,
    sync::{Arc, RwLock},
};

use log::debug;
use rand::prelude::*;
use small_db::{
    btree::{
        self,
        buffer_pool::BufferPool,
        page::{
            BTreeInternalPage, BTreeLeafPage, BTreeLeafPageIteratorRc, BTreePage, BTreePageID,
            Entry,
        },
    },
    common::Catalog,
    storage::tuple::{Cell, Tuple},
    transaction::{ConcurrentStatus, Permission, Transaction},
    utils::{self, HandyRwLock},
    BTreeTable, Database, TableSchema,
};

use super::internal_children_cap;
use crate::test_utils::new_int_tuples;

pub const TEST_DB: &str = "test";

/// # Conduct the initialization
///
/// - Setting up log configurations.
/// - Clear buffer pool.
/// - Reset page size.
/// - Reset log manager.
pub fn setup() {
    utils::init_log();

    // Remote the data directory, ignore the error
    let _ = fs::remove_dir_all("./data");

    Database::reset();

    // increase lock acquisition timeout for benchmark
    if cfg!(feature = "benchmark") {
        ConcurrentStatus::set_timeout(30);
    }

    // print_features();
}

/// Simulate crash.
/// 1. restart Database
pub fn crash() {
    Database::reset();
}

#[derive(Clone, Copy, Debug)]
pub enum TreeLayout {
    Naturally,
    EvenlyDistributed,
    LastTwoEvenlyDistributed,
}

pub fn new_empty_btree_table(table_name: &str, columns: usize) -> Arc<RwLock<BTreeTable>> {
    let schema = TableSchema::small_int_schema(columns);
    let table_rc = Arc::new(RwLock::new(BTreeTable::new(table_name, None, &schema)));
    Catalog::add_table(Arc::clone(&table_rc), true);
    return table_rc;
}

/// Create a table with a given number of rows and columns. All values are
/// random i64.
///
/// This API will reset the log file before returning so there will be no log
/// records left after calling this function.
///
/// # Arguments:
///
/// - int_tuples: This is a reference used to return all inserted data. Only
///   works when it's not None.
///
/// TODO: create the tree using "insert_tuple" api
pub fn new_random_btree_table(
    columns: usize,
    rows: usize,
    result_tuples: Option<&mut Vec<Vec<Cell>>>,
    key_field: usize,
    tree_layout: TreeLayout,
) -> Arc<RwLock<BTreeTable>> {
    let schema = TableSchema::small_int_schema(columns);
    let table_rc = Arc::new(RwLock::new(BTreeTable::new(TEST_DB, None, &schema)));
    Catalog::add_table(Arc::clone(&table_rc), true);

    let write_tx = Transaction::new();

    let mut rng = rand::thread_rng();
    for _ in 0..rows {
        let insert_value = rng.gen_range(i64::MIN, i64::MAX);
        let tuple = new_int_tuples(insert_value, columns, &write_tx);

        let table = table_rc.rl();
        table.insert_tuple(&write_tx, &tuple).unwrap();
    }

    write_tx.commit().unwrap();

    // TODO: remove this block
    {
        Database::mut_buffer_pool().flush_all_pages(&mut Database::mut_log_manager());
    }

    return table_rc;
}
