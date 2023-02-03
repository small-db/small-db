mod test_utils;
use small_db::{
    btree::page::BTreePage, transaction::Transaction,
    utils::HandyRwLock, Tuple, Unique,
};
use test_utils::TreeLayout;

#[test]
fn test_patch() {
    test_utils::setup();

    // Create an empty B+ tree file keyed on the second field of a
    // 2-field tuple.
    let table_rc = test_utils::create_random_btree_table(
        2,
        0,
        None,
        1,
        TreeLayout::Naturally,
    );
    let table = table_rc.rl();

    // step 1: start a transaction
    let tx = Transaction::new();
    tx.start().unwrap();

    // step 2: insert a tuple into the table
    let tuple = Tuple::new_btree_tuple(1, 2);
    table.insert_tuple(&tx, &tuple).unwrap();

    // step 3: force flush all pages (from the buffer pool to disk)
    Unique::buffer_pool().flush_all_pages();

    // step 4: insert another tuple into the table
    let tuple = Tuple::new_btree_tuple(2, 2);
    table.insert_tuple(&tx, &tuple).unwrap();

    // step 5: commit the transaction
    tx.commit().unwrap();

    // check that BufferPool.flushPage() calls LogFile.logWrite().
    assert_eq!(Unique::log_file().records_count(), 5);

    // check that BufferPool.transactionComplete(commit=true) called
    // Page.setBeforeImage(). table.draw_tree(-1);
    // table.check_integrity(true);
    let page_pod = test_utils::get_leaf_page(&table, 0, 0);
    let page = page_pod.rl();
    assert_eq!(page.get_page_data(), page.get_before_image());
}

// #[test]
fn test_abort() {
    test_utils::setup();

    let table_rc = test_utils::create_random_btree_table(
        2,
        0,
        None,
        1,
        TreeLayout::Naturally,
    );
    let table = table_rc.rl();

    let tx = Transaction::new();
    tx.start().unwrap();

    let tuple = Tuple::new_btree_tuple(1, 2);
    table.insert_tuple(&tx, &tuple).unwrap();
    let tuple = Tuple::new_btree_tuple(2, 2);
    table.insert_tuple(&tx, &tuple).unwrap();

    tx.commit().unwrap();

    let tx = Transaction::new();
    tx.start().unwrap();

    let tuple = Tuple::new_btree_tuple(4, 2);
    table.insert_tuple(&tx, &tuple).unwrap();
    let tuple = Tuple::new_btree_tuple(-1, 2);
    table.insert_tuple(&tx, &tuple).unwrap();

    assert!(test_utils::key_present(&tx, &table, 4));
    assert!(test_utils::key_present(&tx, &table, -1));

    tx.abort().unwrap();

    assert!(test_utils::key_present(&tx, &table, 1));
    assert!(test_utils::key_present(&tx, &table, 2));
    assert!(!test_utils::key_present(&tx, &table, 3));
    assert!(!test_utils::key_present(&tx, &table, 4));
    assert!(!test_utils::key_present(&tx, &table, -1));

    //     // insert tuples
    //     void doInsert(HeapFile hf, int t1, int t2)
    //     throws DbException, TransactionAbortedException,
    // IOException {     Transaction t = new Transaction();
    //     t.start();
    //     if(t1 != -1)
    //         insertRow(hf, t, t1, 0);
    //     Database.getBufferPool().flushAllPages();
    //     if(t2 != -1)
    //         insertRow(hf, t, t2, 0);
    //     t.commit();
    // }

    //    // insert tuples
    // // force dirty pages to disk, defeating NO-STEAL
    // // abort
    // void dontInsert(HeapFile hf, int t1, int t2)
    //     throws DbException, TransactionAbortedException,
    // IOException {     Transaction t = new Transaction();
    //     t.start();
    //     if(t1 != -1)
    //         insertRow(hf, t, t1, 0);
    //     if(t2 != -1)
    //         insertRow(hf, t, t2, 0);
    //     if(t1 != -1)
    //         look(hf, t, t1, true);
    //     if(t2 != -1)
    //         look(hf, t, t2, true);
    //     abort(t);
    // }

    // @Test public void TestAbort()
    // throws IOException, DbException, TransactionAbortedException {
    // setup();
    // doInsert(hf1, 1, 2);

    // // *** Test:
    // // insert, abort: data should not be there
    // // flush pages directly to heap file to defeat NO-STEAL policy

    // dontInsert(hf1, 4, -1);

    // Transaction t = new Transaction();
    // t.start();
    // look(hf1, t, 1, true);
    // look(hf1, t, 2, true);
    // look(hf1, t, 3, false);
    // look(hf1, t, 4, false);
    // t.commit();
    // }
}
