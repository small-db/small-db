use rand::Rng;
use small_db::{
    btree::{
        buffer_pool::BufferPool,
        table::{BTreeTableIterator, BTreeTableSearchIterator},
    },
    storage::tuple::Cell,
    transaction::Transaction,
    utils::{ceil_div, HandyRwLock},
    Op, Predicate,
};

use crate::test_utils::{
    assert_true, get_internal_page, get_leaf_page, insert_tuples, internal_children_cap,
    leaf_records_cap, new_int_tuples, new_random_btree_table, search_key, setup, TreeLayout,
};

#[test]
fn test_insert_tuple() {
    setup();

    // Create an empty B+ tree file keyed on the second field of a
    // 2-field tuple.
    let table_rc = new_random_btree_table(2, 0, None, 1, TreeLayout::Naturally);
    let table = table_rc.rl();

    let mut insert_value = 0;

    // write a fullfilled leaf page
    let mut insert_count = leaf_records_cap();
    let tx = Transaction::new();
    for _ in 0..insert_count {
        let tuple = new_int_tuples(insert_value, 2, &tx);
        table.insert_tuple(&tx, &tuple).unwrap();
        insert_value += 1;
        assert_eq!(1, table.pages_count());
    }

    // the next half-paged tuples should live on page 2 since they are
    // greater than all existing tuples in the file
    insert_count = ceil_div(leaf_records_cap(), 2);
    for _ in 0..insert_count {
        let tuple = new_int_tuples(insert_value, 2, &tx);
        table.insert_tuple(&tx, &tuple).unwrap();
        insert_value += 1;

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, table.pages_count());
    }

    // one more insert should cause page 2 to split
    let tuple = new_int_tuples(insert_value, 2, &tx);
    table.insert_tuple(&tx, &tuple).unwrap();

    // there are 4 pages: 1 root page + 3 leaf pages
    assert_true(table.pages_count() == 4, &table);

    // now make sure the records are sorted on the key field
    let it = BTreeTableIterator::new(&tx, &table);
    for (i, tuple) in it.enumerate() {
        assert_eq!(Cell::Int64(i as i64), tuple.get_cell(0));
    }

    tx.commit().unwrap();
}

#[test]
fn test_insert_duplicate_tuples() {
    setup();

    // create an empty B+ tree file keyed on the second field of a
    // 2-field tuple
    let table_rc = new_random_btree_table(2, 0, None, 1, TreeLayout::Naturally);
    let table = table_rc.rl();

    // add a bunch of identical tuples
    let tx = Transaction::new();
    let repetition_count = 600;
    for i in 0..5 {
        for _ in 0..repetition_count {
            let tuple = new_int_tuples(i, 2, &tx);
            table.insert_tuple(&tx, &tuple).unwrap();
        }
    }

    // now search for some ranges and make sure we find all the tuples
    let predicate = Predicate::new(table.key_field, Op::Equals, &Cell::Int64(1));
    let it = BTreeTableSearchIterator::new(&tx, &table, &predicate);
    assert_eq!(it.count(), repetition_count);

    let predicate = Predicate::new(table.key_field, Op::GreaterThanOrEq, &Cell::Int64(2));
    let it = BTreeTableSearchIterator::new(&tx, &table, &predicate);
    assert_eq!(it.count(), repetition_count * 3);

    let predicate = Predicate::new(table.key_field, Op::LessThan, &Cell::Int64(2));
    let it = BTreeTableSearchIterator::new(&tx, &table, &predicate);
    assert_eq!(it.count(), repetition_count * 2);

    tx.commit().unwrap();
}

#[test]
fn test_split_leaf_page() {
    setup();

    // This should create a B+ tree with one full page
    let table_rc = new_random_btree_table(
        2,
        leaf_records_cap(),
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );
    let table = table_rc.rl();

    // there should be 1 leaf page
    assert_eq!(1, table.pages_count());

    // now insert a tuple
    insert_tuples(&table, 1);

    // there should now be 2 leaf pages + 1 internal page
    assert_eq!(3, table.pages_count());

    let root_pod = get_internal_page(&table, 0, 0);
    assert_true(
        root_pod.rl().empty_slots_count() == internal_children_cap() - 2,
        &table,
    );

    // each child should have half of the records
    let leaf_pod = get_leaf_page(&table, 1, 0);
    assert_true(
        leaf_pod.rl().empty_slots_count() <= leaf_records_cap() / 2,
        &table,
    );
    let right_pod = get_leaf_page(&table, 1, 1);
    assert_true(
        right_pod.rl().empty_slots_count() <= leaf_records_cap() / 2,
        &table,
    );
}

#[test]
fn test_split_root_page() {
    setup();

    // This should create a B+ tree which the second tier is packed.
    let row_count = internal_children_cap() * leaf_records_cap();
    let table_rc = new_random_btree_table(2, row_count, None, 0, TreeLayout::EvenlyDistributed);
    let table = table_rc.rl();

    // there should be a packed 2nd layer + 1 internal page (root)
    assert_true(table.pages_count() == internal_children_cap() + 1, &table);
    table.draw_tree(1);

    insert_tuples(&table, 1);

    // there should be 3 internal pages now, since the origianl root
    // page split into 2 pages + 1 new root page
    // and there is also a new leaf page
    assert_true(
        table.pages_count() == internal_children_cap() + 3 + 1,
        &table,
    );

    // the root page should be an internal page and have 2
    // children (1 entry)
    let root_pod = get_internal_page(&table, 0, 0);
    assert_true(
        root_pod.rl().empty_slots_count() == internal_children_cap() - 2,
        &table,
    );

    // each child should have be stable
    let leaf_pod = get_internal_page(&table, 1, 0);
    assert_true(leaf_pod.rl().stable(), &table);
    let right_pod = get_internal_page(&table, 1, 1);
    assert_true(right_pod.rl().stable(), &table);

    // now insert some random tuples and make sure we can find them
    let tx = Transaction::new();

    let mut rng = rand::thread_rng();
    for _ in 0..10000 {
        let insert_value = rng.gen_range(0, i64::MAX);
        let tuple = new_int_tuples(insert_value, 2, &tx);
        table.insert_tuple(&tx, &tuple).unwrap();

        assert_true(search_key(&table, &tx, &tuple.get_cell(0)) >= 1, &table);
    }

    tx.commit().unwrap();
}

#[test]
fn test_split_internal_page() {
    // Use a small page size to speed up the test.
    BufferPool::set_page_size(1024);

    setup();

    // Create a B+ tree with 2 pages in the first tier; the second and
    // the third tier are packed.
    let row_count = 2 * internal_children_cap() * leaf_records_cap();
    let table_rc = new_random_btree_table(2, row_count, None, 0, TreeLayout::EvenlyDistributed);

    let table = table_rc.rl();

    // the number of internal pages is 3
    // the number of leaf pages is 2 * internal_children_cap()
    assert_true(
        table.pages_count() == 3 + 2 * internal_children_cap(),
        &table,
    );

    // now make sure we have enough records and they are all in sorted
    // order
    let tx = Transaction::new();
    let it = BTreeTableIterator::new(&tx, &table);
    let mut previous = Cell::Int64(i64::MIN);
    let mut count: usize = 0;
    for t in it {
        count += 1;

        let current = t.get_cell(table.key_field);
        if current < previous {
            panic!(
                "records are not sorted, i: {}, pre: {:?}, cur: {:?}",
                count, previous, current
            );
        }

        previous = current;
    }

    assert_eq!(count, row_count);

    // now insert some random tuples and make sure we can find them
    let mut rng = rand::thread_rng();
    let rows_increment = 100;
    for _i in 0..rows_increment {
        let insert_value = rng.gen_range(0, i64::MAX);
        let tuple = new_int_tuples(insert_value, 2, &tx);
        table.insert_tuple(&tx, &tuple).unwrap();

        assert_true(search_key(&table, &tx, &tuple.get_cell(0)) >= 1, &table);
    }

    // now make sure we have enough records and they are all in sorted
    // order
    let it = BTreeTableIterator::new(&tx, &table);
    let mut previous = Cell::Int64(i64::MIN);
    let mut count: usize = 0;
    for t in it {
        count += 1;

        let current = t.get_cell(table.key_field);
        if current < previous {
            panic!(
                "records are not sorted, i: {}, pre: {:?}, cur: {:?}",
                count, previous, current
            );
        }

        previous = current;
    }

    assert_eq!(count, row_count + rows_increment);

    tx.commit().unwrap();
}

#[test]
fn test_debug() {
    setup();

    // create an empty B+ tree file keyed on the second field of a
    // 2-field tuple
    let table_rc = new_random_btree_table(2, 0, None, 1, TreeLayout::Naturally);
    let table = table_rc.rl();

    // add a bunch of identical tuples
    let tx = Transaction::new();
    let repetition_count = 100;

    for i in 0..3 {
        for _ in 0..repetition_count {
            let tuple = new_int_tuples(i, 2, &tx);
            table.insert_tuple(&tx, &tuple).unwrap();
        }
    }
    tx.commit().unwrap();

    table.check_integrity();
}
