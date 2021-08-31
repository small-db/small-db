use log::info;
use simple_db_rust::{btree::buffer_pool::BufferPool, *};
use std::{cell::RefCell, rc::Rc};
mod common;

#[test]
fn insert_tuple() {
    common::setup();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = "btree.db";
    let row_scheme = test_utils::simple_int_tuple_scheme(2, "");
    let table_ref = Rc::new(RefCell::new(BTreeTable::new(path, 1, row_scheme)));
    Catalog::global().add_table(Rc::clone(&table_ref));
    let table = table_ref.borrow();

    let mut insert_value = 0;

    // we should be able to add 502 tuples on one page
    let mut insert_count = 502;
    info!("start insert, count: {}", insert_count);
    for _ in 0..insert_count {
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(tuple);
        insert_value += 1;
        assert_eq!(1, table.pages_count());
    }

    // the next 251 tuples should live on page 2 since they are greater than
    // all existing tuples in the file
    insert_count = 251;
    info!("start insert, count: {}", insert_count);
    for _ in 0..insert_count {
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(tuple);
        insert_value += 1;

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, table.pages_count());
    }

    // one more insert greater than 502 should cause page 2 to split
    info!("start insert, count: {}", 1);
    let tuple = Tuple::new_btree_tuple(insert_value, 2);
    table.insert_tuple(tuple);

    // there are 4 pages: 1 root page + 3 leaf pages
    assert_eq!(4, table.pages_count());

    // now make sure the records are sorted on the key field
    let it = table.iterator();
    for (i, tuple) in it.enumerate() {
        assert_eq!(i, tuple.get_field(0).value as usize);
    }
}

#[test]
fn insert_duplicate_tuples() {
    common::setup();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = "btree.db";
    let row_scheme = test_utils::simple_int_tuple_scheme(2, "");
    let table_ref = Rc::new(RefCell::new(BTreeTable::new(path, 1, row_scheme)));
    Catalog::global().add_table(Rc::clone(&table_ref));
    let table = table_ref.borrow();

    // add a bunch of identical tuples
    let repetition_count = 600;
    for i in 0..5 {
        for _ in 0..repetition_count {
            let tuple = Tuple::new_btree_tuple(i, 2);
            table.insert_tuple(tuple);
        }
    }

    // now search for some ranges and make sure we find all the tuples
    let predicate = Predicate::new(Op::Equals, field::IntField::new(1));
    let it = btree::file::BTreeTableSearchIterator::new(&table, predicate);
    assert_eq!(it.count(), repetition_count);

    let predicate =
        Predicate::new(Op::GreaterThanOrEq, field::IntField::new(2));
    let it = btree::file::BTreeTableSearchIterator::new(&table, predicate);
    assert_eq!(it.count(), repetition_count * 3);

    let predicate = Predicate::new(Op::LessThan, field::IntField::new(2));
    let it = btree::file::BTreeTableSearchIterator::new(&table, predicate);
    assert_eq!(it.count(), repetition_count * 2);
}

#[test]
fn split_leaf_page() {
    common::setup();

    // This should create a B+ tree with one full page
    let table_ref = btree::toolkit::create_random_btree_table(2, 502);
    let table = table_ref.borrow();
    table.set_split_strategy(btree::file::SplitStrategy::MoveHalfToRight);

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
    let mut it = btree::page::BTreeInternalPageIterator::new(&root);
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
    let table_ref = btree::toolkit::create_random_btree_table(2, rows);
    let table = table_ref.borrow();

    // there should be 504 leaf pages + 1 internal node
    // assert_eq!(505, table.pages_count());
    info!("pages count: {}", table.pages_count());

    // TODO: remove this check block.
    {
        let it = table.iterator();
        assert_eq!(it.count(), rows as usize);

        let root_pid = table.get_root_pid();
        let root_ref =
            BufferPool::global().get_internal_page(&root_pid).unwrap();
        let root = root_ref.borrow();
        info!("root empty slot count: {}", root.empty_slots_count());
        let it = btree::page::BTreeInternalPageIterator::new(&root);
        info!("root entries count: {}", it.count());
    }

    // now insert a tuple
    // BufferPool::global().insert_tuple(table_id, t)
}

// public void testSplitRootPage() throws Exception {
//     // This should create a packed B+ tree with no empty slots
//     // There are 503 keys per internal page (504 children) and 502 tuples per
// leaf page     // 504 * 502 = 253008
//     BTreeFile bigFile = BTreeUtility.createRandomBTreeFile(2, 253008,
//             null, null, 0);

//     // we will need more room in the buffer pool for this test
//     Database.resetBufferPool(500);

//     // there should be 504 leaf pages + 1 internal node
//     assertEquals(505, bigFile.numPages());

//     // now insert a tuple
//     Database.getBufferPool().insertTuple(tid, bigFile.getId(),
// BTreeUtility.getBTreeTuple(10, 2));

//     // there should now be 505 leaf pages + 3 internal nodes
//     assertEquals(508, bigFile.numPages());

//     // the root node should be an internal node and have 2 children (1 entry)
//     BTreePageId rootPtrPid = new BTreePageId(bigFile.getId(), 0,
// BTreePageId.ROOT_PTR);     BTreeRootPtrPage rootPtr = (BTreeRootPtrPage)
// Database.getBufferPool().getPage(tid, rootPtrPid, Permissions.READ_ONLY);
//     BTreePageId rootId = rootPtr.getRootId();
//     assertEquals(rootId.pgcateg(), BTreePageId.INTERNAL);
//     BTreeInternalPage root = (BTreeInternalPage)
// Database.getBufferPool().getPage(tid, rootId, Permissions.READ_ONLY);
//     assertEquals(502, root.getNumEmptySlots());

//     // each child should have half of the entries
//     Iterator<BTreeEntry> it = root.iterator();
//     assertTrue(it.hasNext());
//     BTreeEntry e = it.next();
//     BTreeInternalPage leftChild = (BTreeInternalPage)
// Database.getBufferPool().getPage(tid, e.getLeftChild(),
// Permissions.READ_ONLY);     BTreeInternalPage rightChild =
// (BTreeInternalPage) Database.getBufferPool().getPage(tid, e.getRightChild(),
// Permissions.READ_ONLY);     assertTrue(leftChild.getNumEmptySlots() <= 252);
//     assertTrue(rightChild.getNumEmptySlots() <= 252);

//     // now insert some random tuples and make sure we can find them
//     Random rand = new Random();
//     for (int i = 0; i < 100; i++) {
//         int item = rand.nextInt(BTreeUtility.MAX_RAND_VALUE);
//         Tuple t = BTreeUtility.getBTreeTuple(item, 2);
//         Database.getBufferPool().insertTuple(tid, bigFile.getId(), t);

//         IndexPredicate ipred = new IndexPredicate(Op.EQUALS, t.getField(0));
//         DbFileIterator fit = bigFile.indexIterator(tid, ipred);
//         fit.open();
//         boolean found = false;
//         while (fit.hasNext()) {
//             if (fit.next().equals(t)) {
//                 found = true;
//                 break;
//             }
//         }
//         fit.close();
//         assertTrue(found);
//     }
// }
