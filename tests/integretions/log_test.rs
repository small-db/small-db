use small_db::{
    btree::page::BTreePage, transaction::Transaction,
    utils::HandyRwLock, BTreeTable, Tuple, Unique,
};

use crate::test_utils::{
    assert_true, get_leaf_page, look_for, new_empty_btree_table,
    new_random_btree_table, setup, TreeLayout,
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
    Unique::mut_page_cache()
        .flush_all_pages(&mut Unique::mut_log_manager());

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
    let table_rc =
        new_random_btree_table(2, 0, None, 1, TreeLayout::Naturally);
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

    let table_rc =
        new_random_btree_table(2, 0, None, 1, TreeLayout::Naturally);
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

    let table_pod_1 = new_empty_btree_table("table_1.db", 2);
    let table_1 = table_pod_1.rl();
    let table_pod_2 = new_empty_btree_table("table_2.db", 2);
    let table_2 = table_pod_2.rl();

    commit_insert(&table_1, 1, 2);

    // Unique::log_file().show_log_contents();
    // return;

    // T1 start, T2 start and commit, T1 abort

    let tx_1 = Transaction::new();
    tx_1.start().unwrap();
    insert_row(&table_1, &tx_1, 3);

    let tx_2 = Transaction::new();
    tx_2.start().unwrap();
    insert_row(&table_2, &tx_2, 21);
    Unique::mut_log_manager().log_checkpoint().unwrap();
    insert_row(&table_2, &tx_2, 22);
    tx_2.commit().unwrap();

    tx_1.abort().unwrap();

    // verify the result
    let tx = Transaction::new();
    assert_true(look_for(&table_1, &tx, 1) == 1, &table_1);
    assert_true(look_for(&table_1, &tx, 2) == 1, &table_1);
    assert_true(look_for(&table_1, &tx, 3) == 0, &table_1);
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
