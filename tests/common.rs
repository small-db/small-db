use log::info;
use rand::prelude::*;
use std::{cell::RefCell, fs, rc::Rc};

use simple_db_rust::{
    btree::{
        buffer_pool::BufferPool,
        page::{
            BTreeInternalPage, BTreeLeafPageIteratorRc, BTreePageID, Entry,
        },
        tuple::TupleScheme,
    },
    util::simple_int_tuple_scheme,
    *,
};

pub const DB_FILE: &str = "./btree.db";

pub fn setup() {
    test_utils::init_log();
    btree::buffer_pool::BufferPool::global().clear();
    fs::remove_file(DB_FILE).unwrap();
}

pub enum TreeLayout {
    Naturally,
    EvenlyDistributed,
    LastTwoEvenlyDistributed,
}

/**
Create a table with a given number of rows and columns.

The rows are filled with random data and are sorted by the
key field/column before being inserted into the table.

The rows are inserted to pages in a compact manner. Result
in all leaf pages being full.
*/
pub fn create_random_btree_table(
    columns: usize,
    rows: usize,
    int_tuples: Option<&mut Vec<Vec<i32>>>,
    key_field: usize,
    tree_layout: TreeLayout,
) -> Rc<RefCell<BTreeTable>> {
    let row_scheme = simple_int_tuple_scheme(columns, "");
    let table_rc = Rc::new(RefCell::new(BTreeTable::new(
        DB_FILE,
        key_field,
        &row_scheme,
    )));
    Catalog::global().add_table(Rc::clone(&table_rc));

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut rng = rand::thread_rng();
    for _ in 0..rows {
        let insert_value = rng.gen_range(i32::MIN, i32::MAX);
        let tuple = Tuple::new_btree_tuple(insert_value, columns);
        tuples.push(tuple);
    }

    tuples.sort_by(|a, b| a.get_field(key_field).cmp(&b.get_field(key_field)));

    if let Some(int_tuples) = int_tuples {
        for t in tuples.iter() {
            let mut row = Vec::new();
            for i in 0..columns {
                row.push(t.get_field(i).value);
            }
            int_tuples.push(row);
        }
    }

    // borrow of table_rc start here
    {
        let table = table_rc.borrow();
        match tree_layout {
            TreeLayout::Naturally => {
                for t in tuples.iter() {
                    table.insert_tuple(t);
                }
            }
            TreeLayout::EvenlyDistributed
            | TreeLayout::LastTwoEvenlyDistributed => {
                let page_index = sequential_insert_into_table(
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

    return table_rc;
}

fn sequential_insert_into_table(
    table: &BTreeTable,
    tuples: &Vec<Tuple>,
    tuple_scheme: &TupleScheme,
    tree_layout: TreeLayout,
) -> usize {
    // stage 1: write leaf pages

    let mut leaf_page_count;
    let mut leaves = Vec::new();
    let mut rows_counts = Vec::new();
    match tree_layout {
        TreeLayout::Naturally => {
            panic!("TreeLayout::Naturally not supported");
        }
        TreeLayout::EvenlyDistributed => {
            let mut rows_per_page: usize =
                BufferPool::rows_per_page(tuple_scheme);
            leaf_page_count = tuples.len() / rows_per_page;
            if tuples.len() % rows_per_page > 0 {
                leaf_page_count += 1;
                rows_per_page = tuples.len() / leaf_page_count;
            }
            for _ in 0..leaf_page_count {
                rows_counts.push(rows_per_page);
            }
        }
        TreeLayout::LastTwoEvenlyDistributed => {
            let rows_per_page: usize = BufferPool::rows_per_page(tuple_scheme);
            leaf_page_count = tuples.len() / rows_per_page;
            let remainder = tuples.len() % rows_per_page;
            if remainder > 0 {
                leaf_page_count += 1;
                let last_tuples_count = remainder + rows_per_page;
                for _ in 0..leaf_page_count - 2 {
                    rows_counts.push(rows_per_page);
                }
                rows_counts.push(last_tuples_count / 2);
                rows_counts.push(last_tuples_count - last_tuples_count / 2);
            } else {
                for _ in 0..leaf_page_count {
                    rows_counts.push(rows_per_page);
                }
            }
        }
    }

    let mut page_index = 0;
    let mut tuple_index = 0;
    for row_count in rows_counts {
        page_index += 1;
        let pid = BTreePageID::new(
            btree::page::PageCategory::Leaf,
            table.get_id(),
            page_index,
        );
        table.write_page_to_disk(&pid);

        let leaf_rc = BufferPool::global().get_leaf_page(&pid).unwrap();
        leaves.push(leaf_rc.clone());
        // borrow of leaf_rc start here
        {
            let mut leaf = leaf_rc.borrow_mut();

            for _ in 0..row_count {
                let t = tuples.get(tuple_index);
                match t {
                    Some(t) => {
                        leaf.insert_tuple(t);
                    }
                    None => {}
                }

                tuple_index += 1;

                // page index in range of [1, leaf_page_count], inclusive

                // set sibling for all but the last leaf page
                if page_index < leaf_page_count {
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
            let leaf = leaves[0].borrow();
            table.set_root_pid(&leaf.get_pid());
            return page_index;
        }
        _ => {}
    }

    // stage 2: write internal pages

    let childrent_per_internal_page = BufferPool::children_per_page();
    let entries_per_internal_page = childrent_per_internal_page - 1;

    let mut internal_page_count = leaf_page_count / childrent_per_internal_page;
    if leaf_page_count % childrent_per_internal_page > 0 {
        internal_page_count =
            (leaf_page_count / childrent_per_internal_page) + 1;
    }

    // leaf index in the leaves vector
    let mut leaf_index = 0;

    let mut internals = Vec::new();
    for i in 0..internal_page_count {
        page_index += 1;
        let pid = BTreePageID::new(
            btree::page::PageCategory::Internal,
            table.get_id(),
            page_index,
        );
        table.write_page_to_disk(&pid);

        let internal_rc = BufferPool::global().get_internal_page(&pid).unwrap();
        internals.push(internal_rc.clone());

        let mut entries_count = entries_per_internal_page;
        if leaves.len() < entries_per_internal_page + 1 {
            entries_count = leaves.len() - 1;
        }
        for j in 0..entries_count {
            // borrow of internal_rc start here
            {
                let left_rc = leaves[leaf_index].clone();
                let right_rc = leaves[leaf_index + 1].clone();
                let mut it = BTreeLeafPageIteratorRc::new(right_rc.clone());
                let key = it.next().unwrap().get_field(table.key_field);

                let mut internal = internal_rc.borrow_mut();
                let mut e = Entry::new(
                    key,
                    &left_rc.borrow().get_pid(),
                    &right_rc.borrow().get_pid(),
                );
                internal.insert_entry(&mut e);

                leaf_index += 1;

                // set parent for all left children
                left_rc.borrow_mut().set_parent_pid(&pid);
                // set parent for the last right child
                if j == entries_count - 1 {
                    right_rc.borrow_mut().set_parent_pid(&pid);
                }
            }
            // borrow of internal_rc ends here
        }

        // increase for the last right child
        leaf_index += 1;
    }

    return write_internal_pages(table, internals, &mut page_index);
}

fn write_internal_pages(
    table: &BTreeTable,
    internals: Vec<Rc<RefCell<BTreeInternalPage>>>,
    page_index: &mut usize,
) -> usize {
    let childrent_per_internal_page = BufferPool::children_per_page();
    if internals.len() <= 1 {
        let internal = internals[0].borrow();
        table.set_root_pid(&internal.get_pid());
        return *page_index;
    } else if internals.len() <= childrent_per_internal_page {
        // write a new internal page (the root page)
        *page_index += 1;
        let pid = BTreePageID::new(
            btree::page::PageCategory::Internal,
            table.get_id(),
            *page_index,
        );
        table.write_page_to_disk(&pid);

        let root_rc = BufferPool::global().get_internal_page(&pid).unwrap();

        // insert entries
        let entries_count = internals.len() - 1;
        for i in 0..entries_count {
            // borrow of root_rc start here
            {
                let left_rc = internals[i].clone();
                let right_rc = internals[i + 1].clone();

                // borrow of right_rc start here
                let key = table
                    .get_last_tuple(&left_rc.borrow().get_pid())
                    .unwrap()
                    .get_field(table.key_field);
                // borrow of right_rc ends here

                let mut root = root_rc.borrow_mut();
                let mut e = Entry::new(
                    key,
                    &left_rc.borrow().get_pid(),
                    &right_rc.borrow().get_pid(),
                );
                info!("inserting entry: {}", e);
                root.insert_entry(&mut e);

                // set parent for all left children
                left_rc.borrow_mut().set_parent_pid(&pid);
                // set parent for the last right child
                if i == entries_count - 1 {
                    right_rc.borrow_mut().set_parent_pid(&pid);
                }
            }
            // borrow of root_rc ends here
        }

        // update root pointer
        table.set_root_pid(&pid);
        return *page_index;
    } else {
        todo!()
    }
}
