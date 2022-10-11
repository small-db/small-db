use log::debug;
use rand::Rng;
use simple_db_rust::{
    btree::{
        buffer_pool::{BufferPool, DEFAULT_PAGE_SIZE},
        page::{BTreeInternalPageIterator, PageCategory},
        table::BTreeTableIterator,
    },
    *,
};
use std::{cell::RefCell, rc::Rc};
mod common;
use common::TreeLayout;

#[test]
fn insert_tuple() {
    common::setup();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let row_scheme = test_utils::simple_int_tuple_scheme(2, "");
    let table_rc = Rc::new(RefCell::new(BTreeTable::new(
        common::DB_FILE,
        1,
        &row_scheme,
    )));
    Catalog::global().add_table(Rc::clone(&table_rc));
    let table = table_rc.borrow();

    let mut insert_value = 0;

    // we should be able to add 502 tuples on one page
    let mut insert_count = 502;
    debug!("start insert, count: {}", insert_count);
    for _ in 0..insert_count {
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(&tuple);
        insert_value += 1;
        assert_eq!(1, table.pages_count());
    }

    // the next 251 tuples should live on page 2 since they are greater than
    // all existing tuples in the file
    insert_count = 251;
    debug!("start insert, count: {}", insert_count);
    for _ in 0..insert_count {
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(&tuple);
        insert_value += 1;

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, table.pages_count());
    }

    // one more insert greater than 502 should cause page 2 to split
    debug!("start insert, count: {}", 1);
    let tuple = Tuple::new_btree_tuple(insert_value, 2);
    table.insert_tuple(&tuple);

    // there are 4 pages: 1 root page + 3 leaf pages
    assert_eq!(4, table.pages_count());

    // now make sure the records are sorted on the key field
    let it = BTreeTableIterator::new(&table);
    for (i, tuple) in it.enumerate() {
        assert_eq!(i, tuple.get_field(0).value as usize);
    }
}

#[test]
fn insert_duplicate_tuples() {
    common::setup();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let row_scheme = test_utils::simple_int_tuple_scheme(2, "");
    let table_ref = Rc::new(RefCell::new(BTreeTable::new(
        common::DB_FILE,
        1,
        &row_scheme,
    )));
    Catalog::global().add_table(Rc::clone(&table_ref));
    let table = table_ref.borrow();

    // add a bunch of identical tuples
    let repetition_count = 600;
    for i in 0..5 {
        for _ in 0..repetition_count {
            let tuple = Tuple::new_btree_tuple(i, 2);
            table.insert_tuple(&tuple);
        }
    }

    // now search for some ranges and make sure we find all the tuples
    let predicate = Predicate::new(Op::Equals, field::IntField::new(1));
    let it = btree::table::BTreeTableSearchIterator::new(&table, predicate);
    assert_eq!(it.count(), repetition_count);

    let predicate =
        Predicate::new(Op::GreaterThanOrEq, field::IntField::new(2));
    let it = btree::table::BTreeTableSearchIterator::new(&table, predicate);
    assert_eq!(it.count(), repetition_count * 3);

    let predicate = Predicate::new(Op::LessThan, field::IntField::new(2));
    let it = btree::table::BTreeTableSearchIterator::new(&table, predicate);
    assert_eq!(it.count(), repetition_count * 2);
}

#[test]
fn split_leaf_page() {
    common::setup();

    // This should create a B+ tree with one full page
    let table_rc = common::create_random_btree_table(
        2,
        502,
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );
    let table = table_rc.borrow();

    // there should be 1 leaf page
    assert_eq!(1, table.pages_count());

    // now insert a tuple
    BufferPool::global()
        .insert_tuple(table.get_id(), Tuple::new_btree_tuple(5000, 2));

    // there should now be 2 leaf pages + 1 internal node
    assert_eq!(3, table.pages_count());

    let root_pid = table.get_root_pid();
    let root_ref = BufferPool::global().get_internal_page(&root_pid).unwrap();
    let root = root_ref.borrow();
    assert_eq!(502, root.empty_slots_count());

    // each child should have half of the records
    let mut it = BTreeInternalPageIterator::new(&root);
    let entry = it.next().unwrap();
    let left_ref = BufferPool::global()
        .get_leaf_page(&entry.get_left_child())
        .unwrap();
    assert!(left_ref.borrow().empty_slots_count() <= 251);

    let right_ref = BufferPool::global()
        .get_leaf_page(&entry.get_right_child())
        .unwrap();
    assert!(right_ref.borrow().empty_slots_count() <= 251);
}

#[test]
fn split_root_page() {
    common::setup();

    // This should create a packed B+ tree with no empty slots
    // There are 503 keys per internal page (504 children) and 502 tuples per
    // leaf page 504 * 502 = 253008
    let rows = 504 * 502;
    let table_ref = common::create_random_btree_table(
        2,
        rows,
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );
    let table = table_ref.borrow();

    // there should be 504 leaf pages + 1 internal node
    assert_eq!(505, table.pages_count());

    // TODO: remove this check block.
    {
        let it = BTreeTableIterator::new(&table);
        assert_eq!(it.count(), rows as usize);

        let root_pid = table.get_root_pid();
        let root_ref =
            BufferPool::global().get_internal_page(&root_pid).unwrap();
        let root = root_ref.borrow();
        debug!("root empty slot count: {}", root.empty_slots_count());
        let it = BTreeInternalPageIterator::new(&root);
        debug!("root entries count: {}", it.count());
    }

    // now insert a tuple
    BufferPool::global()
        .insert_tuple(table.get_id(), Tuple::new_btree_tuple(10, 2));

    // there should now be 505 leaf pages + 3 internal nodes
    assert_eq!(508, table.pages_count());

    // put borrow of pages in a scope so the external process will not
    // be disturbed by the borrow
    {
        // the root node should be an internal node and have 2 children (1
        // entry)
        let root_pid = table.get_root_pid();
        assert_eq!(root_pid.category, PageCategory::Internal);

        let root_page_rc =
            BufferPool::global().get_internal_page(&root_pid).unwrap();
        let root_page = root_page_rc.borrow();
        assert_eq!(root_page.empty_slots_count(), 502);

        // each child should have half of the entries
        let mut it = BTreeInternalPageIterator::new(&root_page);
        let entry = it.next().unwrap();
        let left_pid = entry.get_left_child();
        let left_rc =
            BufferPool::global().get_internal_page(&left_pid).unwrap();
        let left = left_rc.borrow();
        debug!("left entries count: {}", left.entries_count());
        assert!(left.empty_slots_count() <= 252);

        let right_pid = entry.get_right_child();
        let right_rc =
            BufferPool::global().get_internal_page(&right_pid).unwrap();
        let right = right_rc.borrow();
        debug!("right entries count: {}", right.entries_count());
        assert!(right.empty_slots_count() <= 252);
    }

    // now insert some random tuples and make sure we can find them
    let mut rng = rand::thread_rng();
    for _ in 0..10000 {
        let insert_value = rng.gen_range(0, i32::MAX);
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        BufferPool::global().insert_tuple(table.get_id(), tuple.clone());

        let predicate = Predicate::new(Op::Equals, tuple.get_field(0));
        let it = btree::table::BTreeTableSearchIterator::new(&table, predicate);
        let mut found = false;
        for t in it {
            if *t == tuple {
                found = true;
                break;
            }
        }

        assert!(found);
    }
}

#[test]
fn split_internal_page() {
    common::setup();

    // For this test we will decrease the size of the Buffer Pool pages
    BufferPool::set_page_size(1024);

    // This should create a B+ tree with a packed second tier of internal pages
    // and packed third tier of leaf pages
    // (124 tuples per leaf page, 125 children per internal page ->
    // 2 * 125 * 124 = 31000)
    // 2 = 2 children (internal pages) for the top level internal page
    // 125 = 125 children (leaf pages) for each second level internal pages
    // 124 = 124 tuples per leaf page
    let rows = 2 * 125 * 124;
    let table_rc = common::create_random_btree_table(
        2,
        rows,
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );

    let table = table_rc.borrow();

    // there should be 250 leaf pages + 3 internal nodes
    assert_eq!(253, table.pages_count());

    // now make sure we have 31100 records and they are all in sorted order
    let it = BTreeTableIterator::new(&table);
    let mut pre: i32 = i32::MIN;
    let mut count: usize = 0;
    for t in it {
        count += 1;

        let cur = t.get_field(table.key_field).value;
        if t.get_field(table.key_field).value < pre {
            panic!(
                "records are not sorted, i: {}, pre: {}, cur: {}",
                count, pre, cur
            );
        }

        pre = cur;
    }

    assert_eq!(count, rows);

    // now insert some random tuples and make sure we can find them
    let mut rng = rand::thread_rng();
    let rows_increment = 100;
    for _i in 0..rows_increment {
        let insert_value = rng.gen_range(0, i32::MAX);
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(&tuple);

        let predicate = Predicate::new(Op::Equals, tuple.get_field(0));
        let it = btree::table::BTreeTableSearchIterator::new(&table, predicate);
        let mut found = false;
        for t in it {
            if *t == tuple {
                found = true;
                break;
            }
        }

        assert!(found);
    }

    // now make sure we have 31100 records and they are all in sorted order
    let it = BTreeTableIterator::new(&table);
    let mut pre: i32 = i32::MIN;
    let mut count: usize = 0;
    for t in it {
        count += 1;

        let cur = t.get_field(table.key_field).value;
        if t.get_field(table.key_field).value < pre {
            panic!(
                "records are not sorted, i: {}, pre: {}, cur: {}",
                count, pre, cur
            );
        }

        pre = cur;
    }

    assert_eq!(count, rows + rows_increment);
}
