use common::TreeLayout;
use simple_db_rust::btree::{
    buffer_pool::BufferPool, page::PageCategory, table::BTreeTableIterator,
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

// public void testDeleteRootPage() throws Exception {
//     // This should create a B+ tree with two half-full leaf pages
//     BTreeFile twoLeafPageFile = BTreeUtility.createRandomBTreeFile(2, 503,
//             null, null, 0);
//     // there should be one internal node and 2 leaf nodes
//     assertEquals(3, twoLeafPageFile.numPages());
//     BTreeChecker.checkRep(twoLeafPageFile,
//             tid, new HashMap<PageId, Page>(), true);

//     // delete the first two tuples
//     DbFileIterator it = twoLeafPageFile.iterator(tid);
//     it.open();
//     Tuple first = it.next();
//     Tuple second = it.next();
//     it.close();
//     twoLeafPageFile.deleteTuple(tid, first);
//     BTreeChecker.checkRep(twoLeafPageFile, tid, new HashMap<PageId, Page>(),
// false);     twoLeafPageFile.deleteTuple(tid, second);
//     BTreeChecker.checkRep(twoLeafPageFile,tid, new HashMap<PageId, Page>(),
// false);

//     // confirm that the last two pages have merged successfully and replaced
// the root     BTreePageId rootPtrId =
// BTreeRootPtrPage.getId(twoLeafPageFile.getId());     BTreeRootPtrPage rootPtr
// = (BTreeRootPtrPage) Database.getBufferPool().getPage(             tid,
// rootPtrId, Permissions.READ_ONLY);     assertTrue(rootPtr.getRootId().
// pgcateg() == BTreePageId.LEAF);     BTreeLeafPage root = (BTreeLeafPage)
// Database.getBufferPool().getPage(             tid, rootPtr.getRootId(),
// Permissions.READ_ONLY);     assertEquals(1, root.getNumEmptySlots());
//     assertTrue(root.getParentId().equals(rootPtrId));
// }
