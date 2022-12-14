mod test_utils;
use log::{debug, error};
use small_db::{
    btree::{
        buffer_pool::BufferPool,
        page::{BTreeInternalPageIterator, BTreePage, PageCategory},
        table::BTreeTableIterator,
    },
    concurrent_status::Permission,
    transaction::Transaction,
    utils::{ceil_div, floor_div, HandyRwLock},
    BTreeTable, Op, Tuple, Unique,
};
use test_utils::TreeLayout;

use crate::test_utils::{
    get_internal_page, get_leaf_page, internal_children_cap,
    internal_entries_cap, leaf_records_cap,
};

#[test]
fn test_redistribute_leaf_pages() {
    test_utils::setup();

    // Create a B+ tree with two full leaf pages.
    let table_pod = test_utils::create_random_btree_table(
        2,
        leaf_records_cap() * 2,
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );
    let table = table_pod.rl();

    table.draw_tree(-1);
    table.check_integrity(true);

    let left_pod = get_leaf_page(&table, 1, 0);
    let right_pod = get_leaf_page(&table, 1, 1);

    // Delete some tuples from the first page until it gets to minimum
    // occupancy.
    let count = ceil_div(leaf_records_cap(), 2);
    delete_tuples(&table, count);
    table.draw_tree(-1);
    table.check_integrity(true);
    assert_eq!(left_pod.rl().empty_slots_count(), count);

    // Deleting a tuple now should bring the page below minimum
    // occupancy and cause the tuples to be redistributed.
    delete_tuples(&table, 1);
    assert!(left_pod.rl().empty_slots_count() <= count);

    // Assert some tuples of the right page were stolen.
    assert!(right_pod.rl().empty_slots_count() > 0);

    table.draw_tree(-1);
    table.check_integrity(true);
}

#[test]
fn test_merge_leaf_pages() {
    let ctx = test_utils::setup();

    // This should create a B+ tree with one three half-full leaf
    // pages
    let table_rc = test_utils::create_random_btree_table(
        2,
        1005,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_rc.rl();

    table.draw_tree(-1);
    table.check_integrity(true);

    // delete the last two tuples
    let mut it = BTreeTableIterator::new(&ctx.tx, &table);
    let _ = table.delete_tuple(&ctx.tx, &it.next_back().unwrap());
    let _ = table.delete_tuple(&ctx.tx, &it.next_back().unwrap());

    table.draw_tree(-1);
    table.check_integrity(true);
}

#[test]
fn test_delete_root_page() {
    let ctx = test_utils::setup();

    // this should create a B+ tree with two half-full leaf pages
    let table_rc = test_utils::create_random_btree_table(
        2,
        leaf_records_cap() * 2,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_rc.rl();
    table.draw_tree(-1);
    table.check_integrity(true);
    // there should be one internal node and 2 leaf nodes
    assert_eq!(3, table.pages_count());

    // delete the first two tuples
    delete_tuples(&table, leaf_records_cap());

    table.check_integrity(true);
    table.draw_tree(-1);
    let root_pod = get_leaf_page(&table, 0, 0);
    assert_eq!(root_pod.rl().empty_slots_count(), 0);
}

#[test]
fn test_reuse_deleted_pages() {
    let ctx = test_utils::setup();

    // this should create a B+ tree with 3 leaf nodes
    let table_rc = test_utils::create_random_btree_table(
        2,
        1005,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_rc.rl();
    table.check_integrity(true);

    // 3 leaf pages, 1 internal page
    assert_eq!(4, table.pages_count());

    // delete enough tuples to ensure one page gets deleted
    let it = BTreeTableIterator::new(&ctx.tx, &table);
    for t in it.take(502) {
        table.delete_tuple(&ctx.tx, &t).unwrap();
    }

    // now there should be 2 leaf pages, 1 internal page, 1 unused
    // leaf page, 1 header page
    assert_eq!(5, table.pages_count());

    // insert enough tuples to ensure one of the leaf pages splits
    for value in 0..502 {
        let tuple = Tuple::new_btree_tuple(value, 2);
        table.insert_tuple(&ctx.tx, &tuple).unwrap();
    }
    ctx.tx.commit().unwrap();

    // now there should be 3 leaf pages, 1 internal page, and 1 header
    // page
    assert_eq!(5, table.pages_count());
}

#[test]
fn test_redistribute_internal_pages() {
    let ctx = test_utils::setup();

    // This should create a B+ tree with two nodes in the second tier
    // and 602 nodes in the third tier.
    //
    // 302204 = 2 * 301 * 502
    // 2 internal pages
    // 602 leaf pages
    let table_rc = test_utils::create_random_btree_table(
        2,
        302204,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_rc.rl();
    table.check_integrity(true);
    table.draw_tree(-1);

    // bring the left internal page to minimum occupancy
    let mut it = BTreeTableIterator::new(&ctx.tx, &table);
    for t in it.by_ref().take(49 * 502 + 1) {
        table.delete_tuple(&ctx.tx, &t).unwrap();
    }

    table.draw_tree(-1);
    table.check_integrity(true);

    // deleting a page of tuples should bring the internal page below
    // minimum occupancy and cause the entries to be redistributed
    for t in it.by_ref().take(502) {
        if let Err(e) = table.delete_tuple(&ctx.tx, &t) {
            error!("Error: {:?}", e);
            table.draw_tree(-1);
            table.check_integrity(true);
        }
    }

    table.draw_tree(-1);
    table.check_integrity(true);
}

#[test]
fn test_delete_internal_pages() {
    let ctx = test_utils::setup();

    BufferPool::set_page_size(1024);

    // This should create a B+ tree with three nodes in the second
    // tier and third tier is packed.
    let row_count = 3 * internal_children_cap() * leaf_records_cap();
    let table_rc = test_utils::create_random_btree_table(
        2,
        row_count,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );

    let table = table_rc.rl();
    table.draw_tree(2);
    table.check_integrity(true);

    let root_pod = get_internal_page(&table, 0, 0);
    assert_eq!(3, root_pod.rl().children_count());

    // Delete tuples causing leaf pages to merge until the first
    // internal page gets to minimum occupancy.
    let count =
        ceil_div(internal_children_cap(), 2) * leaf_records_cap();
    delete_tuples(&table, count);

    // Deleting a page of tuples should bring the internal page below
    // minimum occupancy and cause the entries to be redistributed.
    table.draw_tree(2);
    table.check_integrity(true);
    let left_child_pod = get_internal_page(&table, 1, 0);
    assert_eq!(
        internal_children_cap() / 2,
        left_child_pod.rl().empty_slots_count(),
    );

    let count = (ceil_div(internal_children_cap(), 2) - 1)
        * leaf_records_cap();
    delete_tuples(&table, count);

    table.draw_tree(2);
    table.check_integrity(true);

    // deleting another page of tuples should bring the page below
    // minimum occupancy again but this time cause it to merge
    // with its right sibling
    let count = leaf_records_cap();
    delete_tuples(&table, count);

    // confirm that the pages have merged
    let root_pod = get_internal_page(&table, 0, 0);
    table.draw_tree(2);
    table.check_integrity(true);
    assert_eq!(2, root_pod.rl().children_count());

    let e = BTreeInternalPageIterator::new(&root_pod.rl())
        .next()
        .unwrap();
    let left_child_pod = get_internal_page(&table, 1, 0);
    let right_child_pod = get_internal_page(&table, 1, 1);
    assert_eq!(0, left_child_pod.rl().empty_slots_count());
    assert!(e.get_key().compare(
        Op::LessThanOrEq,
        BTreeInternalPageIterator::new(&right_child_pod.rl())
            .next()
            .unwrap()
            .get_key()
    ));

    let count = internal_children_cap() * leaf_records_cap();
    delete_tuples(&table, count);

    // confirm that the last two internal pages have merged
    // successfully and replaced the root
    let root_pod = get_internal_page(&table, 0, 0);
    assert_eq!(0, root_pod.rl().empty_slots_count());
    table.draw_tree(2);
    table.check_integrity(true);
}

fn delete_tuples(table: &BTreeTable, count: usize) {
    let tx = Transaction::new();
    let mut it = BTreeTableIterator::new(&tx, &table);
    for _ in 0..count {
        table.delete_tuple(&tx, &it.next().unwrap()).unwrap();
    }
    tx.commit().unwrap();
}
