mod test_utils;
use log::error;
use small_db::{
    btree::{
        buffer_pool::BufferPool,
        page::{BTreeInternalPageIterator, BTreePage},
        table::BTreeTableIterator,
    },
    utils::{ceil_div, floor_div, HandyRwLock},
    Op,
};
use test_utils::TreeLayout;

use crate::test_utils::{
    delete_tuples, get_internal_page, get_leaf_page, insert_tuples,
    internal_children_cap, leaf_records_cap,
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

    let left_pod = get_leaf_page(&table, 1, 0);
    let right_pod = get_leaf_page(&table, 1, 1);

    // Delete some tuples from the first page until it gets to minimum
    // occupancy.
    let delete_count = floor_div(leaf_records_cap(), 2);
    delete_tuples(&table, delete_count);
    test_utils::assert_true(
        left_pod.rl().empty_slots_count() == delete_count,
        &table,
    );

    // Deleting a tuple now should bring the page below minimum
    // occupancy and cause the tuples to be redistributed.
    delete_tuples(&table, 1);
    test_utils::assert_true(
        left_pod.rl().empty_slots_count() < delete_count,
        &table,
    );

    // Assert some tuples of the right page were stolen.
    // assert!(right_pod.rl().empty_slots_count() > 0);
    test_utils::assert_true(
        right_pod.rl().empty_slots_count() > 0,
        &table,
    );
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
    table
        .delete_tuple(&ctx.tx, &it.next_back().unwrap())
        .unwrap();
    table
        .delete_tuple(&ctx.tx, &it.next_back().unwrap())
        .unwrap();

    table.draw_tree(-1);
    table.check_integrity(true);
}

#[test]
fn test_delete_root_page() {
    test_utils::setup();

    // this should create a B+ tree with two full leaf pages
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
    test_utils::setup();

    // This should create a B+ tree with 3 leaf nodes.
    let table_rc = test_utils::create_random_btree_table(
        2,
        leaf_records_cap() * 3,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_rc.rl();
    table.draw_tree(-1);
    table.check_integrity(true);

    // 3 leaf pages, 1 internal page
    assert_eq!(4, table.pages_count());

    // delete enough tuples to ensure one page gets deleted
    delete_tuples(&table, leaf_records_cap() + 2);

    // now there should be 2 leaf pages, 1 internal page, 1 unused
    // leaf page, 1 header page
    table.draw_tree(-1);
    table.check_integrity(true);
    assert_eq!(5, table.pages_count());

    // insert enough tuples to ensure one of the leaf pages splits
    insert_tuples(&table, leaf_records_cap());

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
    test_utils::setup();

    BufferPool::set_page_size(1024);

    // Create a B+ tree with 3 nodes in the first tier; the second and
    // the third tier are packed.
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
    let second_child_pod = get_internal_page(&table, 1, 1);

    assert_eq!(3, root_pod.rl().children_count());

    // Delete tuples causing leaf pages to merge until the first
    // internal page gets to minimum occupancy.
    let count =
        ceil_div(internal_children_cap(), 2) * leaf_records_cap();
    delete_tuples(&table, count);
    assert_eq!(second_child_pod.rl().empty_slots_count(), 0);

    // Deleting two pages of tuples should bring the internal page
    // below minimum occupancy and cause the entries to be
    // redistributed.
    let count = 2 * leaf_records_cap();
    delete_tuples(&table, count);
    assert!(second_child_pod.rl().empty_slots_count() > 0);

    // Deleting another page of tuples should bring the page below
    // minimum occupancy again but this time cause it to merge
    // with its right sibling.
    let count =
        ceil_div(internal_children_cap(), 2) * leaf_records_cap();
    delete_tuples(&table, count);

    // Confirm that the pages have merged.
    assert_eq!(2, root_pod.rl().children_count());

    let e = BTreeInternalPageIterator::new(&root_pod.rl())
        .next()
        .unwrap();
    let first_child_pod = get_internal_page(&table, 1, 0);
    let second_child_pod = get_internal_page(&table, 1, 1);
    table.draw_tree(2);
    table.check_integrity(true);
    assert!(e.get_key().compare(
        Op::LessThanOrEq,
        BTreeInternalPageIterator::new(&second_child_pod.rl())
            .next()
            .unwrap()
            .get_key()
    ));

    let count =
        first_child_pod.rl().children_count() * leaf_records_cap();
    delete_tuples(&table, count);

    // Confirm that the last two internal pages have merged
    // successfully and replaced the root.
    let root_pod = get_internal_page(&table, 0, 0);
    assert_eq!(0, root_pod.rl().empty_slots_count());
    table.draw_tree(2);
    table.check_integrity(true);
}
