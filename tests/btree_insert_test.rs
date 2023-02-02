mod test_utils;
use std::sync::Arc;

use log::debug;
use rand::Rng;
use small_db::{
    btree::{
        buffer_pool::BufferPool,
        table::{BTreeTableIterator, BTreeTableSearchIterator},
    },
    utils::{ceil_div, HandyRwLock},
    *,
};
use test_utils::TreeLayout;

use crate::test_utils::{
    insert_tuples, internal_children_cap, leaf_records_cap,
};

#[test]
fn test_insert_tuple() {
    let ctx = test_utils::setup();

    // create an empty B+ tree file keyed on the second field of a
    // 2-field tuple
    let table_rc = test_utils::create_random_btree_table(
        2,
        0,
        None,
        1,
        TreeLayout::Naturally,
    );
    Unique::mut_catalog().add_table(Arc::clone(&table_rc));
    let table = table_rc.rl();

    let mut insert_value = 0;

    // write a fullfilled leaf page
    let mut insert_count = leaf_records_cap();
    debug!("start insert, count: {}", insert_count);
    for _ in 0..insert_count {
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(&ctx.tx, &tuple).unwrap();
        insert_value += 1;
        assert_eq!(1, table.pages_count());
    }

    // the next half-paged tuples should live on page 2 since they are
    // greater than all existing tuples in the file
    insert_count = ceil_div(leaf_records_cap(), 2);
    debug!("start insert, count: {}", insert_count);
    for _ in 0..insert_count {
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(&ctx.tx, &tuple).unwrap();
        insert_value += 1;

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, table.pages_count());
    }

    // one more insert greater than 502 should cause page 2 to split
    debug!("start insert, count: {}", 1);
    let tuple = Tuple::new_btree_tuple(insert_value, 2);
    table.insert_tuple(&ctx.tx, &tuple).unwrap();

    // there are 4 pages: 1 root page + 3 leaf pages
    test_utils::assert_true(table.pages_count() == 4, &table);

    // now make sure the records are sorted on the key field
    let it = BTreeTableIterator::new(&ctx.tx, &table);
    for (i, tuple) in it.enumerate() {
        assert_eq!(i, tuple.get_field(0).value as usize);
    }
}

#[test]
fn test_insert_duplicate_tuples() {
    let ctx = test_utils::setup();

    // create an empty B+ tree file keyed on the second field of a
    // 2-field tuple
    let table_rc = test_utils::create_random_btree_table(
        2,
        0,
        None,
        1,
        TreeLayout::Naturally,
    );
    Unique::mut_catalog().add_table(Arc::clone(&table_rc));
    let table = table_rc.rl();

    // add a bunch of identical tuples
    let repetition_count = 600;
    for i in 0..5 {
        for _ in 0..repetition_count {
            let tuple = Tuple::new_btree_tuple(i, 2);
            table.insert_tuple(&ctx.tx, &tuple).unwrap();
        }
    }

    // now search for some ranges and make sure we find all the tuples
    let predicate =
        Predicate::new(Op::Equals, field::IntField::new(1));
    let it =
        BTreeTableSearchIterator::new(&ctx.tx, &table, predicate);
    assert_eq!(it.count(), repetition_count);

    let predicate =
        Predicate::new(Op::GreaterThanOrEq, field::IntField::new(2));
    let it =
        BTreeTableSearchIterator::new(&ctx.tx, &table, predicate);
    assert_eq!(it.count(), repetition_count * 3);

    let predicate =
        Predicate::new(Op::LessThan, field::IntField::new(2));
    let it = btree::table::BTreeTableSearchIterator::new(
        &ctx.tx, &table, predicate,
    );
    assert_eq!(it.count(), repetition_count * 2);
}

#[test]
fn test_split_leaf_page() {
    test_utils::setup();

    // This should create a B+ tree with one full page
    let table_rc = test_utils::create_random_btree_table(
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
    test_utils::insert_tuples(&table, 1);

    // there should now be 2 leaf pages + 1 internal node
    assert_eq!(3, table.pages_count());

    let root_pod = test_utils::get_internal_page(&table, 0, 0);
    test_utils::assert_true(
        root_pod.rl().empty_slots_count()
            == internal_children_cap() - 2,
        &table,
    );

    // each child should have half of the records
    let leaf_pod = test_utils::get_leaf_page(&table, 1, 0);
    test_utils::assert_true(
        leaf_pod.rl().empty_slots_count() <= leaf_records_cap() / 2,
        &table,
    );
    let right_pod = test_utils::get_leaf_page(&table, 1, 1);
    test_utils::assert_true(
        right_pod.rl().empty_slots_count() <= leaf_records_cap() / 2,
        &table,
    );
}

#[test]
fn test_split_root_page() {
    let ctx = test_utils::setup();

    // This should create a B+ tree which the second tier is packed.
    let row_count = internal_children_cap() * leaf_records_cap();
    let table_rc = test_utils::create_random_btree_table(
        2,
        row_count,
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );
    let table = table_rc.rl();

    // there should be a packed 2nd layer + 1 internal node (root)
    test_utils::assert_true(
        table.pages_count() == internal_children_cap() + 1,
        &table,
    );

    insert_tuples(&table, 1);

    // there should be 3 internal nodes now, since the origianl root
    // node split into 2 nodes + 1 new root node
    // and there is also a new leaf node
    test_utils::assert_true(
        table.pages_count() == internal_children_cap() + 3 + 1,
        &table,
    );

    // the root node should be an internal node and have 2
    // children (1 entry)
    let root_pod = test_utils::get_internal_page(&table, 0, 0);
    test_utils::assert_true(
        root_pod.rl().empty_slots_count()
            == internal_children_cap() - 2,
        &table,
    );

    // each child should have half of the entries
    let leaf_pod = test_utils::get_internal_page(&table, 1, 0);
    test_utils::assert_true(
        leaf_pod.rl().empty_slots_count()
            <= internal_children_cap() / 2,
        &table,
    );
    let right_pod = test_utils::get_internal_page(&table, 1, 1);
    test_utils::assert_true(
        right_pod.rl().empty_slots_count()
            <= internal_children_cap() / 2,
        &table,
    );

    // now insert some random tuples and make sure we can find them
    let mut rng = rand::thread_rng();
    for _ in 0..10000 {
        let insert_value = rng.gen_range(0, i32::MAX);
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(&ctx.tx, &tuple).unwrap();

        let predicate =
            Predicate::new(Op::Equals, tuple.get_field(0));
        let it = btree::table::BTreeTableSearchIterator::new(
            &ctx.tx, &table, predicate,
        );
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
fn test_split_internal_page() {
    let ctx = test_utils::setup();

    // For this test we will decrease the size of the Buffer Pool
    // pages.
    BufferPool::set_page_size(1024);

    // Create a B+ tree with 2 nodes in the first tier; the second and
    // the third tier are packed.
    let row_count = 2 * internal_children_cap() * leaf_records_cap();
    let table_rc = test_utils::create_random_btree_table(
        2,
        row_count,
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );

    let table = table_rc.rl();

    // the number of internal nodes is 3
    // the number of leaf nodes is 2 * internal_children_cap()
    test_utils::assert_true(
        table.pages_count() == 3 + 2 * internal_children_cap(),
        &table,
    );

    // now make sure we have enough records and they are all in sorted
    // order
    let it = BTreeTableIterator::new(&ctx.tx, &table);
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

    assert_eq!(count, row_count);

    // now insert some random tuples and make sure we can find them
    let mut rng = rand::thread_rng();
    let rows_increment = 100;
    for _i in 0..rows_increment {
        let insert_value = rng.gen_range(0, i32::MAX);
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(&ctx.tx, &tuple).unwrap();

        let predicate =
            Predicate::new(Op::Equals, tuple.get_field(0));
        let it = btree::table::BTreeTableSearchIterator::new(
            &ctx.tx, &table, predicate,
        );
        let mut found = false;
        for t in it {
            if *t == tuple {
                found = true;
                break;
            }
        }

        assert!(found);
    }

    // now make sure we have enough records and they are all in sorted
    // order
    let it = BTreeTableIterator::new(&ctx.tx, &table);
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

    assert_eq!(count, row_count + rows_increment);
}
