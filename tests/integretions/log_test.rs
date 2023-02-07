use small_db::{
    btree::page::BTreePage, transaction::Transaction,
    utils::HandyRwLock, BTreeTable, Tuple, Unique,
};

use crate::test_utils::{
    assert_true, create_random_btree_table, get_leaf_page, look_for,
    setup, TreeLayout,
};

fn insert_row(table: &BTreeTable, tx: &Transaction, key: i32) {
    let tuple = Tuple::new_btree_tuple(key, 2);
    table.insert_tuple(&tx, &tuple).unwrap();
}

/// Insert two tuples into the table, then commit the transaction.
/// (There is a flush action in the middle of the transaction.)
fn commit_insert(table: &BTreeTable, key_1: i32, key_2: i32) {
    // step 1: start a transaction
    let tx = Transaction::new();
    tx.start().unwrap();

    // step 2: insert a tuple into the table
    insert_row(&table, &tx, key_1);

    // step 3: force flush all pages (from the buffer pool to disk)
    Unique::buffer_pool().flush_all_pages();

    // step 4: insert another tuple into the table
    insert_row(&table, &tx, key_2);

    // step 5: commit the transaction
    tx.commit().unwrap();
}

/// Insert two tuples into the table, then abort the transaction.
/// (We well look for the tuples before abort.)
fn abort_insert(table: &BTreeTable, key_1: i32, key_2: i32) {
    // step 1: start a transaction
    let tx = Transaction::new();
    tx.start().unwrap();

    // step 2: insert two tuples into the table
    insert_row(&table, &tx, key_1);
    insert_row(&table, &tx, key_2);

    // step 3: search for the tuples
    assert_true(look_for(table, &tx, key_1) == 1, table);
    assert_true(look_for(table, &tx, key_2) == 1, table);

    // step 4: abort the transaction
    if let Err(e) = tx.abort() {
        panic!("abort failed: {}", e);
    }
    // assert_true(tx.abort().is_ok(), table);
}

#[test]
fn test_patch() {
    setup();

    // Create an empty B+ tree file keyed on the second field of a
    // 2-field tuple.
    let table_rc = create_random_btree_table(
        2,
        0,
        None,
        1,
        TreeLayout::Naturally,
    );
    let table = table_rc.rl();

    commit_insert(&table, 1, 2);

    // check that BufferPool.flushPage() calls LogFile.logWrite().
    assert_eq!(Unique::log_file().records_count(), 5);

    // check that BufferPool.transactionComplete(commit=true) called
    // Page.setBeforeImage().
    let page_pod = get_leaf_page(&table, 0, 0);
    let page = page_pod.rl();
    assert_eq!(page.get_page_data(), page.get_before_image());
}

#[test]
fn test_abort() {
    setup();

    let table_rc = create_random_btree_table(
        2,
        0,
        None,
        1,
        TreeLayout::Naturally,
    );
    let table = table_rc.rl();

    // TODO: what's the meaning of below comments?
    //
    // insert, abort: data should not be there
    // flush pages directly to heap file to defeat NO-STEAL policy

    commit_insert(&table, 1, 2);
    abort_insert(&table, 3, 4);

    let tx = Transaction::new();
    assert_true(look_for(&table, &tx, 1) == 1, &table);
    assert_true(look_for(&table, &tx, 2) == 1, &table);
    assert_true(look_for(&table, &tx, 3) == 0, &table);
    assert_true(look_for(&table, &tx, 4) == 0, &table);
    tx.commit().unwrap();
}

#[test]
fn test_abort_commit_interleaved() {
    setup();

    let table_rc = create_random_btree_table(
        2,
        0,
        None,
        1,
        TreeLayout::Naturally,
    );
    let table = table_rc.rl();

    commit_insert(&table, 1, 2);

    // T1 start, T2 start and commit, T1 abort

    let tx_1 = Transaction::new();
    tx_1.start().unwrap();
    insert_row(&table, &tx_1, 3);

    let tx_2 = Transaction::new();
    tx_2.start().unwrap();
    insert_row(&table, &tx_2, 21);
    Unique::mut_log_file().log_checkpoint().unwrap();
    insert_row(&table, &tx_2, 22);
    tx_2.commit().unwrap();

    tx_1.abort().unwrap();

    // verify the result
    let tx = Transaction::new();
    assert_true(look_for(&table, &tx, 1) == 1, &table);
    assert_true(look_for(&table, &tx, 2) == 1, &table);
    assert_true(look_for(&table, &tx, 3) == 0, &table);
    tx.commit().unwrap();

    // Transaction t1 = new Transaction();
    // t1.start();
    // insertRow(hf1, t1, 3);

    // Transaction t2 = new Transaction();
    // t2.start();
    // insertRow(hf2, t2, 21);
    // Database.getLogFile().logCheckpoint();
    // insertRow(hf2, t2, 22);
    // t2.commit();

    // insertRow(hf1, t1, 4);
    // abort(t1);

    // Transaction t = new Transaction();
    // t.start();
    // look(hf1, t, 1, true);
    // look(hf1, t, 2, true);
    // look(hf1, t, 3, false);
    // look(hf1, t, 4, false);
    // look(hf2, t, 21, true);
    // look(hf2, t, 22, true);
    // t.commit();
}
