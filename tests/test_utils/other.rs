use std::sync::{Arc, RwLock};

use log::debug;
use rand::prelude::*;
use small_db::{
    btree::{
        buffer_pool::{BufferPool, DEFAULT_PAGE_SIZE},
        page::{
            BTreeInternalPage, BTreeInternalPageIterator,
            BTreeLeafPage, BTreeLeafPageIteratorRc, BTreePage,
            BTreePageID, Entry,
        },
        tuple::TupleScheme,
    },
    concurrent_status::Permission,
    transaction::Transaction,
    types::Pod,
    utils::{small_int_tuple_scheme, HandyRwLock},
    *,
};

pub const DB_FILE: &str = "./btree.db";

pub struct TestContext {
    pub tx: Transaction,
}

/// # Conduct the initialization
///
/// - Setting up log configurations.
/// - Clear buffer pool.
/// - Reset page size.
pub fn setup() -> TestContext {
    utils::init_log();
    Unique::buffer_pool().clear();
    BufferPool::set_page_size(DEFAULT_PAGE_SIZE);

    let tx = Transaction::new();
    return TestContext { tx };
}

#[derive(Clone, Copy, Debug)]
pub enum TreeLayout {
    Naturally,
    EvenlyDistributed,
    LastTwoEvenlyDistributed,
}

/// Create a table with a given number of rows and columns.
///
/// The rows are filled with random data and are sorted by the
/// key field/column before being inserted into the table.
///
/// The rows are inserted to pages in a compact manner. Result
/// in all leaf pages being full.
///
/// # Arguments:
///
/// - int_tuples: This is a reference used to return all inserted
///   data. Only works when it's not None.
pub fn create_random_btree_table(
    columns: usize,
    rows: usize,
    int_tuples: Option<&mut Vec<Vec<i32>>>,
    key_field: usize,
    tree_layout: TreeLayout,
) -> Arc<RwLock<BTreeTable>> {
    let row_scheme = small_int_tuple_scheme(columns, "");
    let table_rc = Arc::new(RwLock::new(BTreeTable::new(
        DB_FILE,
        key_field,
        &row_scheme,
    )));
    Unique::mut_catalog().add_table(Arc::clone(&table_rc));

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut rng = rand::thread_rng();
    for _ in 0..rows {
        let insert_value = rng.gen_range(i32::MIN, i32::MAX);
        let tuple = Tuple::new_btree_tuple(insert_value, columns);
        tuples.push(tuple);
    }

    tuples.sort_by(|a, b| {
        a.get_field(key_field).cmp(&b.get_field(key_field))
    });

    if let Some(int_tuples) = int_tuples {
        for t in tuples.iter() {
            let mut row = Vec::new();
            for i in 0..columns {
                row.push(t.get_field(i).value);
            }
            int_tuples.push(row);
        }
    }

    let write_tx = Transaction::new();

    // borrow of table_rc start here
    {
        let table = table_rc.rl();
        match tree_layout {
            TreeLayout::Naturally => {
                for t in tuples.iter() {
                    table.insert_tuple(&write_tx, t).unwrap();
                }
            }
            TreeLayout::EvenlyDistributed
            | TreeLayout::LastTwoEvenlyDistributed => {
                let page_index = sequential_insert_into_table(
                    &write_tx,
                    &table,
                    &tuples,
                    &row_scheme,
                    tree_layout,
                );
                table.set_page_index(page_index);
            }
        }
    }
    // borrow of table_rc ends here

    write_tx.commit().unwrap();
    debug!(
        "table construction finished, insert {} rows in total",
        rows,
    );

    return table_rc;
}

fn sequential_insert_into_table(
    tx: &Transaction,
    table: &BTreeTable,
    tuples: &Vec<Tuple>,
    tuple_scheme: &TupleScheme,
    tree_layout: TreeLayout,
) -> u32 {
    // stage 1: write leaf pages
    let mut leaves = Vec::new();

    let leaf_buckets = get_buckets(
        tuples.len(),
        BTreeLeafPage::calculate_slots_count(&tuple_scheme),
        tree_layout,
    );

    let mut page_index = 0;
    let mut tuple_index = 0;
    for tuple_count in &leaf_buckets {
        page_index += 1;
        let pid = BTreePageID::new(
            btree::page::PageCategory::Leaf,
            table.get_id(),
            page_index,
        );
        table.write_empty_page_to_disk(&pid);

        let leaf_rc = Unique::buffer_pool()
            .get_leaf_page(tx, Permission::ReadWrite, &pid)
            .unwrap();
        leaves.push(leaf_rc.clone());
        // borrow of leaf_rc start here
        {
            let mut leaf = leaf_rc.wl();

            for _ in 0..*tuple_count {
                if let Some(t) = tuples.get(tuple_index) {
                    leaf.insert_tuple(t);
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
    let interanl_buckets = get_buckets(
        leaf_buckets.len(),
        internal_children_cap(),
        tree_layout,
    );

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

        let internal_rc = Unique::buffer_pool()
            .get_internal_page(tx, Permission::ReadWrite, &pid)
            .unwrap();
        internals.push(internal_rc.clone());

        let entries_count = children_count - 1;
        for j in 0..entries_count {
            // borrow of internal_rc start here
            {
                let left_rc = leaves[leaf_index].clone();
                let right_rc = leaves[leaf_index + 1].clone();
                let mut it =
                    BTreeLeafPageIteratorRc::new(right_rc.clone());
                let key =
                    it.next().unwrap().get_field(table.key_field);

                let mut internal = internal_rc.wl();
                let mut e = Entry::new(
                    key,
                    &left_rc.rl().get_pid(),
                    &right_rc.rl().get_pid(),
                );
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

    return write_internal_pages(
        tx,
        table,
        internals,
        &mut page_index,
    );
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

        let root_rc = Unique::buffer_pool()
            .get_internal_page(tx, Permission::ReadWrite, &pid)
            .unwrap();

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
                    .get_field(table.key_field);
                // borrow of right_rc ends here

                let mut root = root_rc.wl();
                let mut e = Entry::new(
                    key,
                    &left_rc.rl().get_pid(),
                    &right_rc.rl().get_pid(),
                );
                root.insert_entry(&mut e);

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

fn get_buckets(
    elem_count: usize,
    max_capacity: usize,
    layout: TreeLayout,
) -> Vec<usize> {
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
            for _ in
                0..(bucket_count.checked_sub(2).unwrap_or_default())
            {
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

pub fn leaf_records_cap() -> usize {
    let scheme = small_int_tuple_scheme(2, "");
    BTreeLeafPage::calculate_slots_count(&scheme)
}

pub fn internal_entries_cap() -> usize {
    BTreeInternalPage::get_children_cap(4)
}

pub fn internal_children_cap() -> usize {
    BTreeInternalPage::get_children_cap(4) + 1
}

pub fn get_internal_page(
    table: &BTreeTable,
    level: usize,
    index: usize,
) -> Pod<BTreeInternalPage> {
    let tx = Transaction::new();
    let root_pid = table.get_root_pid(&tx);
    let root_pod = Unique::buffer_pool()
        .get_internal_page(&tx, Permission::ReadOnly, &root_pid)
        .unwrap();

    match level {
        0 => {
            tx.commit().unwrap();
            return root_pod;
        }
        1 => match index {
            0 => {
                let e =
                    BTreeInternalPageIterator::new(&root_pod.rl())
                        .next()
                        .unwrap();
                let left_child_rc = Unique::buffer_pool()
                    .get_internal_page(
                        &tx,
                        Permission::ReadOnly,
                        &e.get_left_child(),
                    )
                    .unwrap();
                tx.commit().unwrap();
                return left_child_rc;
            }
            _ => {
                let e =
                    BTreeInternalPageIterator::new(&root_pod.rl())
                        .skip(index - 1)
                        .next()
                        .unwrap();
                let left_child_rc = Unique::buffer_pool()
                    .get_internal_page(
                        &tx,
                        Permission::ReadOnly,
                        &e.get_right_child(),
                    )
                    .unwrap();
                tx.commit().unwrap();
                return left_child_rc;
            }
        },
        _ => todo!(),
    }
}

pub fn get_leaf_page(
    table: &BTreeTable,
    level: usize,
    index: usize,
) -> Pod<BTreeLeafPage> {
    match level {
        0 => {
            let tx = Transaction::new();
            let root_pid = table.get_root_pid(&tx);
            let root_pod = Unique::buffer_pool()
                .get_leaf_page(&tx, Permission::ReadOnly, &root_pid)
                .unwrap();
            tx.commit().unwrap();
            return root_pod;
        }
        _ => {
            let internal_pod =
                get_internal_page(table, level - 1, index);
            let tx = Transaction::new();
            let e =
                BTreeInternalPageIterator::new(&internal_pod.rl())
                    .next()
                    .unwrap();
            let leaf_pod = Unique::buffer_pool()
                .get_leaf_page(
                    &tx,
                    Permission::ReadOnly,
                    &e.get_left_child(),
                )
                .unwrap();
            tx.commit().unwrap();
            return leaf_pod;
        }
    }
}
