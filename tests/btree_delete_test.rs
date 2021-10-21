use common::TreeLayout;
use simple_db_rust::{
    btree::{
        buffer_pool::BufferPool, table::BTreeTableIterator,
    },
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

// public void testMergeLeafPages() throws Exception {
//     // This should create a B+ tree with one full page and two half-full leaf
// pages     BTreeFile threeLeafPageFile = BTreeUtility.createRandomBTreeFile(2,
// 1005,             null, null, 0);

//     BTreeChecker.checkRep(threeLeafPageFile,
//             tid, new HashMap<PageId, Page>(), true);
//     // there should be one internal node and 3 leaf nodes
//     assertEquals(4, threeLeafPageFile.numPages());

//     // delete the last two tuples
//     DbFileIterator it = threeLeafPageFile.iterator(tid);
//     it.open();
//     Tuple secondToLast = null;
//     Tuple last = null;
//     while(it.hasNext()) {
//         secondToLast = last;
//         last = it.next();
//     }
//     it.close();
//     threeLeafPageFile.deleteTuple(tid, secondToLast);
//     threeLeafPageFile.deleteTuple(tid, last);
//     BTreeChecker.checkRep(threeLeafPageFile, tid, new HashMap<PageId,
// Page>(), true);

//     // confirm that the last two pages have merged successfully
//     BTreePageId rootPtrId =
// BTreeRootPtrPage.getId(threeLeafPageFile.getId());     BTreeRootPtrPage
// rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
// tid, rootPtrId, Permissions.READ_ONLY);     BTreeInternalPage root =
// (BTreeInternalPage) Database.getBufferPool().getPage(             tid,
// rootPtr.getRootId(), Permissions.READ_ONLY);     assertEquals(502,
// root.getNumEmptySlots());     BTreeEntry e = root.iterator().next();
//     BTreeLeafPage leftChild = (BTreeLeafPage)
// Database.getBufferPool().getPage(             tid, e.getLeftChild(),
// Permissions.READ_ONLY);     BTreeLeafPage rightChild = (BTreeLeafPage)
// Database.getBufferPool().getPage(             tid, e.getRightChild(),
// Permissions.READ_ONLY);     assertEquals(0, leftChild.getNumEmptySlots());
//     assertEquals(1, rightChild.getNumEmptySlots());
//     assertTrue(e.getKey().equals(rightChild.iterator().next().getField(0)));

// }
