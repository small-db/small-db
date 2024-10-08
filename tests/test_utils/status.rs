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
use crate::test_utils::{debug::print_features, new_int_tuples};

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

    let mut write_tx = Transaction::new();

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut rng = rand::thread_rng();
    for _ in 0..rows {
        let insert_value = rng.gen_range(i64::MIN, i64::MAX);
        let tuple = new_int_tuples(insert_value, columns, &write_tx);
        tuples.push(tuple);
    }

    tuples.sort_by_cached_key(|t| t.get_cell(key_field));

    if let Some(inner_tuples) = result_tuples {
        for t in tuples.iter() {
            let mut row = Vec::new();
            for i in 0..columns {
                row.push(t.get_cell(i));
            }
            inner_tuples.push(row);
        }
    }

    // borrow of table_rc start here
    {
        let table = table_rc.rl();
        match tree_layout {
            TreeLayout::Naturally => {
                for t in tuples.iter() {
                    table.insert_tuple(&mut write_tx, t).unwrap();
                }
            }
            TreeLayout::EvenlyDistributed | TreeLayout::LastTwoEvenlyDistributed => {
                let page_index =
                    sequential_insert_into_table(&write_tx, &table, &tuples, &schema, tree_layout);
                table.set_page_index(page_index);
            }
        }
    }
    // borrow of table_rc ends here

    write_tx.commit().unwrap();
    debug!("table construction finished, insert {} rows in total", rows,);

    Database::mut_log_manager().reset();

    return table_rc;
}

fn sequential_insert_into_table(
    tx: &Transaction,
    table: &BTreeTable,
    tuples: &Vec<Tuple>,
    schema: &TableSchema,
    tree_layout: TreeLayout,
) -> u32 {
    // stage 1: write leaf pages
    let mut leaves = Vec::new();

    let leaf_buckets = get_buckets(
        tuples.len(),
        BTreeLeafPage::calc_children_cap(&schema),
        tree_layout,
    );

    let mut page_index = 0;
    let mut tuple_index = 0;
    for tuple_count in &leaf_buckets {
        page_index += 1;
        let pid = BTreePageID::new(btree::page::PageCategory::Leaf, table.get_id(), page_index);
        table.write_empty_page_to_disk(&pid);

        let leaf_rc = BufferPool::get_leaf_page(tx, Permission::ReadWrite, &pid).unwrap();
        leaves.push(leaf_rc.clone());
        // borrow of leaf_rc start here
        {
            let mut leaf = leaf_rc.wl();

            for _ in 0..*tuple_count {
                if let Some(t) = tuples.get(tuple_index) {
                    leaf.insert_tuple(t).unwrap();
                }

                tuple_index += 1;

                // page index in range of [1, leaf_page_count],
                // inclusive

                // set sibling for all but the last leaf page
                if page_index < leaf_buckets.len() as u32 {
                    let right_pid = BTreePageID::new(
                        btree::page::PageCategory::Leaf,
                        table.get_id(),
                        page_index + 1,
                    );
                    leaf.set_right_pid(Some(right_pid));
                }

                // set sibling for all but the first leaf page
                if page_index > 1 {
                    let left_pid = BTreePageID::new(
                        btree::page::PageCategory::Leaf,
                        table.get_id(),
                        page_index - 1,
                    );
                    leaf.set_left_pid(Some(left_pid));
                }
            }
        }
        // borrow of leaf_rc ends here
    }

    match leaves.len() {
        0 => {
            return page_index;
        }
        1 => {
            let leaf = leaves[0].rl();
            table.set_root_pid(tx, &leaf.get_pid());
            return page_index;
        }
        _ => {}
    }

    // stage 2: write internal pages
    let interanl_buckets = get_buckets(leaf_buckets.len(), internal_children_cap(), tree_layout);

    // leaf index in the leaves vector
    let mut leaf_index = 0;

    let mut internals = Vec::new();
    for children_count in interanl_buckets {
        page_index += 1;
        let pid = BTreePageID::new(
            btree::page::PageCategory::Internal,
            table.get_id(),
            page_index,
        );
        table.write_empty_page_to_disk(&pid);

        let internal_rc = BufferPool::get_internal_page(tx, Permission::ReadWrite, &pid).unwrap();
        internals.push(internal_rc.clone());

        let entries_count = children_count - 1;
        for j in 0..entries_count {
            // borrow of internal_rc start here
            {
                let left_rc = leaves[leaf_index].clone();
                let right_rc = leaves[leaf_index + 1].clone();
                let mut it = BTreeLeafPageIteratorRc::new(tx, right_rc.clone());
                let key = it.next().unwrap().get_cell(table.key_field);

                let mut internal = internal_rc.wl();
                let mut e = Entry::new(&key, &left_rc.rl().get_pid(), &right_rc.rl().get_pid());
                internal.insert_entry(&mut e).unwrap();

                leaf_index += 1;

                // set parent for all left children
                left_rc.wl().set_parent_pid(&pid);
                // set parent for the last right child
                if j == entries_count - 1 {
                    right_rc.wl().set_parent_pid(&pid);
                }
            }
            // borrow of internal_rc ends here
        }

        // increase for the last right child
        leaf_index += 1;
    }

    return write_internal_pages(tx, table, internals, &mut page_index);
}

fn write_internal_pages(
    tx: &Transaction,
    table: &BTreeTable,
    internals: Vec<Arc<RwLock<BTreeInternalPage>>>,
    page_index: &mut u32,
) -> u32 {
    if internals.len() <= 1 {
        let internal = internals[0].rl();
        table.set_root_pid(tx, &internal.get_pid());
        return *page_index;
    } else if internals.len() <= internal_children_cap() {
        // write a new internal page (the root page)
        *page_index += 1;
        let pid = BTreePageID::new(
            btree::page::PageCategory::Internal,
            table.get_id(),
            *page_index,
        );
        table.write_empty_page_to_disk(&pid);

        let root_rc = BufferPool::get_internal_page(tx, Permission::ReadWrite, &pid).unwrap();

        // insert entries
        let entries_count = internals.len() - 1;
        for i in 0..entries_count {
            // borrow of root_rc start here
            {
                let left_rc = internals[i].clone();
                let right_rc = internals[i + 1].clone();

                // borrow of right_rc start here
                let key = table
                    .get_last_tuple(tx, &left_rc.rl().get_pid())
                    .unwrap()
                    .get_cell(table.key_field);
                // borrow of right_rc ends here

                let mut root = root_rc.wl();
                let mut e = Entry::new(&key, &left_rc.rl().get_pid(), &right_rc.rl().get_pid());
                root.insert_entry(&mut e).unwrap();

                // set parent for all left children
                left_rc.wl().set_parent_pid(&pid);
                // set parent for the last right child
                if i == entries_count - 1 {
                    right_rc.wl().set_parent_pid(&pid);
                }
            }
            // borrow of root_rc ends here
        }

        // update root pointer
        table.set_root_pid(tx, &pid);
        return *page_index;
    } else {
        todo!()
    }
}

fn get_buckets(elem_count: usize, max_capacity: usize, layout: TreeLayout) -> Vec<usize> {
    if elem_count <= max_capacity {
        return vec![elem_count];
    }

    let mut bucket_count = elem_count / max_capacity;
    if elem_count % max_capacity > 0 {
        bucket_count += 1;
    }

    let mut table = Vec::new();
    match layout {
        TreeLayout::Naturally | TreeLayout::EvenlyDistributed => {
            let bucket_size = elem_count / bucket_count;
            let lacked = elem_count % bucket_count;
            for _ in 0..lacked {
                table.push(bucket_size + 1);
            }
            for _ in lacked..bucket_count {
                table.push(bucket_size);
            }
        }
        TreeLayout::LastTwoEvenlyDistributed => {
            let lacked = max_capacity * bucket_count - elem_count;
            for _ in 0..(bucket_count.checked_sub(2).unwrap_or_default()) {
                table.push(max_capacity);
            }

            table.push(max_capacity - lacked / 2);
            if lacked % 2 == 0 {
                table.push(max_capacity - lacked / 2);
            } else {
                table.push(max_capacity - lacked / 2 - 1);
            }
        }
    }

    table
}
