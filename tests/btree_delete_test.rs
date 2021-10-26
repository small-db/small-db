use common::TreeLayout;
use simple_db_rust::{
    btree::{
        buffer_pool::BufferPool, page::PageCategory, table::BTreeTableIterator,
    },
    Tuple,
};

mod common;

#[test]
fn test_redistribute_leaf_pages() {
    common::setup();

    // This should create a B+ tree with two partially full leaf pages
    let table_rc = common::create_random_btree_table(
        2,
        600,
        None,
        0,
        TreeLayout::EvenlyDistributed,
    );
    let table = table_rc.borrow();

    table.draw_tree(-1);
    table.check_integrity(true);

    // Delete some tuples from the first page until it gets to minimum occupancy
    let mut it = BTreeTableIterator::new(&table);
    let mut count = 0;
    let page_rc = table.get_first_page();
    for tuple in it.by_ref() {
        assert_eq!(202 + count, page_rc.borrow().empty_slots_count());

        table.delete_tuple(tuple);

        count += 1;
        if count >= 49 {
            break;
        }
    }

    // deleting a tuple now should bring the page below minimum occupancy and
    // cause the tuples to be redistributed
    let t = it.next().unwrap();
    let page_rc = BufferPool::global().get_leaf_page(&t.get_pid()).unwrap();
    assert_eq!(page_rc.borrow().empty_slots_count(), 251);
    table.delete_tuple(t);
    assert!(page_rc.borrow().empty_slots_count() <= 251);

    let right_pid = page_rc.borrow().get_right_pid().unwrap();
    let right_rc = BufferPool::global().get_leaf_page(&right_pid).unwrap();
    // assert some tuples of the right page were stolen
    assert!(right_rc.borrow().empty_slots_count() > 202);

    table.draw_tree(-1);
    table.check_integrity(true);
}

#[test]
fn test_merge_leaf_pages() {
    common::setup();

    // This should create a B+ tree with one three half-full leaf pages
    let table_rc = common::create_random_btree_table(
        2,
        1005,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_rc.borrow();

    table.draw_tree(-1);
    table.check_integrity(true);

    // delete the last two tuples
    let mut it = BTreeTableIterator::new(&table);
    table.delete_tuple(it.next_back().unwrap());
    table.delete_tuple(it.next_back().unwrap());

    table.draw_tree(-1);
    table.check_integrity(true);
}

#[test]
fn test_delete_root_page() {
    common::setup();

    // This should create a B+ tree with two half-full leaf pages
    let table_rc = common::create_random_btree_table(
        2,
        503,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_rc.borrow();
    assert_eq!(3, table.pages_count());
    table.check_integrity(true);

    // delete the first two tuples
    let mut it = BTreeTableIterator::new(&table);
    table.delete_tuple(it.next().unwrap());
    table.check_integrity(true);
    table.delete_tuple(it.next().unwrap());
    table.check_integrity(true);

    table.draw_tree(-1);

    let root_pid = table.get_root_pid();
    assert!(root_pid.category == PageCategory::Leaf);
    let root_rc = BufferPool::global().get_leaf_page(&root_pid).unwrap();
    assert_eq!(root_rc.borrow().empty_slots_count(), 1);
}

#[test]
fn test_reuse_deleted_pages() {
    common::setup();

    // this should create a B+ tree with 3 leaf nodes
    let table_rc = common::create_random_btree_table(
        2,
        1005,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_rc.borrow();
    table.check_integrity(true);

    // 3 leaf pages, 1 internal page
    assert_eq!(4, table.pages_count());
    table.draw_tree(-1);
    table.check_integrity(true);

    // delete enough tuples to ensure one page gets deleted
    let it = BTreeTableIterator::new(&table);
    for t in it.take(502) {
        table.delete_tuple(t);
    }

    // now there should be 2 leaf pages, 1 internal page, 1 unused leaf page, 1
    // header page
    assert_eq!(5, table.pages_count());
    table.draw_tree(-1);
    table.check_integrity(true);

    // insert enough tuples to ensure one of the leaf pages splits
    for value in 0..502 {
        let tuple = Tuple::new_btree_tuple(value, 2);
        table.insert_tuple(&tuple);
    }

    // now there should be 3 leaf pages, 1 internal page, and 1 header page
    assert_eq!(5, table.pages_count());
    table.draw_tree(-1);
    table.check_integrity(true);
}

// public void testReuseDeletedPages() throws Exception {
//     // this should create a B+ tree with 3 leaf nodes
//     BTreeFile threeLeafPageFile = BTreeUtility.createRandomBTreeFile(2, 1005,
//             null, null, 0);
//     BTreeChecker.checkRep(threeLeafPageFile, tid, new HashMap<PageId,
// Page>(), true);

//     // 3 leaf pages, 1 internal page
//     assertEquals(4, threeLeafPageFile.numPages());

//     // delete enough tuples to ensure one page gets deleted
//     DbFileIterator it = threeLeafPageFile.iterator(tid);
//     it.open();
//     for(int i = 0; i < 502; ++i) {
//         Database.getBufferPool().deleteTuple(tid, it.next());
//         it.rewind();
//     }
//     it.close();

//     // now there should be 2 leaf pages, 1 internal page, 1 unused leaf page,
// 1 header page     assertEquals(5, threeLeafPageFile.numPages());

//     // insert enough tuples to ensure one of the leaf pages splits
//     for(int i = 0; i < 502; ++i) {
//         Database.getBufferPool().insertTuple(tid, threeLeafPageFile.getId(),
//                 BTreeUtility.getBTreeTuple(i, 2));
//     }

//     // now there should be 3 leaf pages, 1 internal page, and 1 header page
//     assertEquals(5, threeLeafPageFile.numPages());
//     BTreeChecker.checkRep(threeLeafPageFile, tid, new HashMap<PageId,
// Page>(), true); }
