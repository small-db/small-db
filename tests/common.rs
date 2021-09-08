use log::info;
use rand::prelude::*;
use std::{cell::RefCell, rc::Rc};

use simple_db_rust::{
    btree::{
        buffer_pool::BufferPool,
        page::{BTreeInternalPage, BTreeLeafPageIterator, BTreePageID, Entry},
        tuple::TupleScheme,
    },
    util::simple_int_tuple_scheme,
    *,
};

pub fn setup() {
    test_utils::init_log();
    btree::buffer_pool::BufferPool::global().clear();
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
    packed_layout: bool,
) -> Rc<RefCell<BTreeTable>> {
    let path = "btree.db";
    let row_scheme = simple_int_tuple_scheme(columns, "");
    let table_rc =
        Rc::new(RefCell::new(BTreeTable::new(path, key_field, &row_scheme)));
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
        if packed_layout {
            let page_index =
                sequential_insert_into_table(&table, &tuples, &row_scheme);
            table.set_page_index(page_index);
        } else {
            for t in tuples.iter() {
                table.insert_tuple(t);
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
) -> usize {
    // write leaf pages
    let leaf_page_count: usize;
    let rows_per_leaf_page: usize = BufferPool::rows_per_page(tuple_scheme);
    let mut leaves = Vec::new();
    if tuples.len() % rows_per_leaf_page > 0 {
        leaf_page_count = (tuples.len() / rows_per_leaf_page) + 1;
    } else {
        leaf_page_count = tuples.len() / rows_per_leaf_page;
    }

    let mut page_index = 0;
    let mut tuple_index = 0;
    for _ in 0..leaf_page_count {
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
            for _ in 0..rows_per_leaf_page {
                let t = tuples.get(tuple_index);
                match t {
                    Some(t) => {
                        leaf.insert_tuple(t);
                    }
                    None => {}
                }

                tuple_index += 1;

                // set right sibling for all but the last leaf page
                if page_index < leaf_page_count {
                    let right_pid = BTreePageID::new(
                        btree::page::PageCategory::Leaf,
                        table.get_id(),
                        page_index + 1,
                    );
                    leaf.set_right_sibling_pid(Some(right_pid));
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

    // write internal pages
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

        for _ in 0..entries_per_internal_page {
            // borrow of internal_rc start here
            {
                let left_rc = leaves[leaf_index].clone();
                let right_rc = leaves[leaf_index + 1].clone();
                let mut it = BTreeLeafPageIterator::new(right_rc.clone());
                let key = it.next().unwrap().get_field(0).value;

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
                if i == internal_page_count - 1 {
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
                    .get_field(table.key_field)
                    .value;
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
