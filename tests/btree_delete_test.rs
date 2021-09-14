use simple_db_rust::btree::{
    buffer_pool::BufferPool, table::BTreeTableIterator,
};

mod common;

#[test]
fn test_redistribute_leaf_pages() {
    common::setup();
    // This should create a B+ tree with two partially full leaf pages
    let table_rc = common::create_random_btree_table(2, 600, None, 0, true);
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

    // deleting a tuple now should bring the page below minimum occupancy and cause
    // the tuples to be redistributed
    let t = it.next().unwrap();
    let page_rc = BufferPool::global().get_leaf_page(&t.get_pid()).unwrap();
    assert_eq!(page_rc.borrow().empty_slots_count(), 251);
    table.delete_tuple(t);
    assert!(page_rc.borrow().empty_slots_count() <= 251);
}

// // This should create a B+ tree with two partially full leaf pages
// BTreeFile twoLeafPageFile = BTreeUtility.createRandomBTreeFile(2, 600,
//         null, null, 0);
// BTreeChecker.checkRep(twoLeafPageFile, tid, new HashMap<PageId, Page>(),
// true);

// // Delete some tuples from the first page until it gets to minimum occupancy
// DbFileIterator it = twoLeafPageFile.iterator(tid);
// it.open();
// int count = 0;
// while(it.hasNext() && count < 49) {
//     Tuple t = it.next();
//     BTreePageId pid = (BTreePageId) t.getRecordId().getPageId();
//     BTreeLeafPage p = (BTreeLeafPage) Database.getBufferPool().getPage(
//             tid, pid, Permissions.READ_ONLY);
//     assertEquals(202 + count, p.getNumEmptySlots());
//     twoLeafPageFile.deleteTuple(tid, t);
//     count++;
// }
// BTreeChecker.checkRep(twoLeafPageFile,tid, new HashMap<PageId, Page>(),
// true);

// // deleting a tuple now should bring the page below minimum occupancy and
// cause // the tuples to be redistributed
// Tuple t = it.next();
// it.close();
// BTreePageId pid = (BTreePageId) t.getRecordId().getPageId();
// BTreeLeafPage p = (BTreeLeafPage) Database.getBufferPool().getPage(
//         tid, pid, Permissions.READ_ONLY);
// assertEquals(251, p.getNumEmptySlots());
// twoLeafPageFile.deleteTuple(tid, t);
// assertTrue(p.getNumEmptySlots() <= 251);

// BTreePageId rightSiblingId = p.getRightSiblingId();
// BTreeLeafPage rightSibling = (BTreeLeafPage)
// Database.getBufferPool().getPage(         tid, rightSiblingId,
// Permissions.READ_ONLY); assertTrue(rightSibling.getNumEmptySlots() > 202);
