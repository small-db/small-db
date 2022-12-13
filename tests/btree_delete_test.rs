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
    utils::HandyRwLock,
    Op, Tuple, Unique,
};
use test_utils::TreeLayout;

use crate::test_utils::{
    get_internal_page, internal_children_count,
    internal_entries_count, leaf_slots_count,
};

#[test]
fn test_redistribute_leaf_pages() {
    let ctx = test_utils::setup();

    // This should create a B+ tree with two partially full leaf pages
    let table_rc = test_utils::create_random_btree_table(
        2,
        600,
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );
    let table = table_rc.rl();

    table.draw_tree(-1);
    table.check_integrity(true);

    // Delete some tuples from the first page until it gets to minimum
    // occupancy
    let mut it = BTreeTableIterator::new(&ctx.tx, &table);
    let mut count = 0;
    let page_rc =
        table.get_first_page(&ctx.tx, Permission::ReadWrite);
    for tuple in it.by_ref() {
        assert_eq!(202 + count, page_rc.rl().empty_slots_count());

        let _ = table.delete_tuple(&ctx.tx, &tuple);

        count += 1;
        if count >= 49 {
            break;
        }
    }

    // deleting a tuple now should bring the page below minimum
    // occupancy and cause the tuples to be redistributed
    let t = it.next().unwrap();
    let page_rc = Unique::buffer_pool()
        .get_leaf_page(&ctx.tx, Permission::ReadOnly, &t.get_pid())
        .unwrap();
    assert_eq!(page_rc.rl().empty_slots_count(), 251);
    let _ = table.delete_tuple(&ctx.tx, &t);
    assert!(page_rc.rl().empty_slots_count() <= 251);

    let _right_pid = page_rc.rl().get_right_pid().unwrap();
    let right_rc = Unique::buffer_pool()
        .get_leaf_page(&ctx.tx, Permission::ReadOnly, &t.get_pid())
        .unwrap();
    // assert some tuples of the right page were stolen
    assert!(right_rc.rl().empty_slots_count() > 202);

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
        503,
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
    let mut it = BTreeTableIterator::new(&ctx.tx, &table);
    table.delete_tuple(&ctx.tx, &it.next().unwrap()).unwrap();
    table.check_integrity(true);
    table.delete_tuple(&ctx.tx, &it.next().unwrap()).unwrap();
    table.check_integrity(true);

    table.draw_tree(-1);

    let root_pid = table.get_root_pid(&ctx.tx);
    assert!(root_pid.category == PageCategory::Leaf);
    let root_rc = Unique::buffer_pool()
        .get_leaf_page(&ctx.tx, Permission::ReadOnly, &root_pid)
        .unwrap();
    assert_eq!(root_rc.rl().empty_slots_count(), 1);
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
    let row_count =
        3 * internal_children_count() * leaf_slots_count();
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
    assert_eq!(2, root_pod.rl().entries_count());

    // Delete tuples causing leaf pages to merge until the first
    // internal page gets to minimum occupancy.
    let tx = Transaction::new();
    let mut it = BTreeTableIterator::new(&tx, &table);
    for _ in 0..(internal_entries_count() / 2) {
        for _ in 0..leaf_slots_count() {
            table.delete_tuple(&tx, &it.next().unwrap()).unwrap();
        }
    }
    tx.commit().unwrap();

    table.draw_tree(2);
    table.check_integrity(true);

    // Deleting a page of tuples should bring the internal page below
    // minimum occupancy and cause the entries to be redistributed.
    let left_child_pod = get_internal_page(&table, 1, 0);
    assert_eq!(
        internal_entries_count() / 2,
        left_child_pod.rl().empty_slots_count(),
    );

    let tx = Transaction::new();
    let mut it = BTreeTableIterator::new(&tx, &table);
    for _ in 0..(internal_entries_count() / 2) {
        for _ in 0..leaf_slots_count() {
            table.delete_tuple(&tx, &it.next().unwrap()).unwrap();
        }
    }
    tx.commit().unwrap();

    let left_child_pod = get_internal_page(&table, 1, 0);
    let right_child_pod = get_internal_page(&table, 1, 1);
    table.draw_tree(2);
    table.check_integrity(true);
    assert_eq!(
        internal_entries_count() / 2,
        left_child_pod.rl().empty_slots_count()
    );
    assert_eq!(
        internal_entries_count() / 2,
        right_child_pod.rl().empty_slots_count()
    );

    // deleting another page of tuples should bring the page below
    // minimum occupancy again but this time cause it to merge
    // with its right sibling
    let it = BTreeTableIterator::new(&ctx.tx, &table);
    for t in it.take(leaf_slots_count()) {
        table.delete_tuple(&ctx.tx, &t).unwrap();
    }

    // confirm that the pages have merged
    table.draw_tree(2);
    table.check_integrity(true);
    assert_eq!(1, root_pod.rl().entries_count());
    let e = BTreeInternalPageIterator::new(&root_pod.rl())
        .next()
        .unwrap();
    let left_child_rc = Unique::buffer_pool()
        .get_internal_page(
            &ctx.tx,
            Permission::ReadWrite,
            &e.get_left_child(),
        )
        .unwrap();
    let right_child_rc = Unique::buffer_pool()
        .get_internal_page(
            &ctx.tx,
            Permission::ReadWrite,
            &e.get_right_child(),
        )
        .unwrap();
    assert_eq!(0, left_child_rc.rl().empty_slots_count());
    assert!(e.get_key().compare(
        Op::LessThanOrEq,
        BTreeInternalPageIterator::new(&right_child_rc.rl())
            .next()
            .unwrap()
            .get_key()
    ));

    // Delete tuples causing leaf pages to merge until the first
    // internal page gets below minimum occupancy and causes the
    // entries to be redistributed
    let mut it = BTreeTableIterator::new(&ctx.tx, &table);
    let mut count = 0;
    for _ in 0..62 {
        assert_eq!(count, left_child_rc.rl().empty_slots_count());
        for _ in 0..124 {
            table.delete_tuple(&ctx.tx, &it.next().unwrap()).unwrap();
        }
        count += 1;
    }

    // deleting another page of tuples should bring the page below
    // minimum occupancy and cause it to merge with the right
    // sibling to replace the root
    let mut it = BTreeTableIterator::new(&ctx.tx, &table);
    for _ in 0..124 {
        table.delete_tuple(&ctx.tx, &it.next().unwrap()).unwrap();
    }

    // confirm that the last two internal pages have merged
    // successfully and replaced the root
    let root_pid = table.get_root_pid(&ctx.tx);
    let root_rc = Unique::buffer_pool()
        .get_internal_page(&ctx.tx, Permission::ReadWrite, &root_pid)
        .unwrap();
    assert_eq!(0, root_rc.rl().empty_slots_count());
    table.draw_tree(2);
    table.check_integrity(true);
}
