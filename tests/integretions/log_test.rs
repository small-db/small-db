use small_db::{
    btree::page::BTreePage, transaction::Transaction,
    utils::HandyRwLock, BTreeTable, Tuple, Unique,
};

use crate::test_utils::{
    assert_true, get_leaf_page, new_empty_btree_table,
    new_random_btree_table, search_key, setup, TreeLayout,
};

fn insert_row(table: &BTreeTable, tx: &Transaction, key: i32) {
    let tuple = Tuple::new_btree_tuple(key, 2);
    table.insert_tuple(&tx, &tuple).unwrap();
}

/// Insert two tuples into the table, then commit the transaction.
/// (There is a flush action in the middle of the transaction.)
fn commit_insert(table: &BTreeTable, key_1: i32, key_2: i32) {
    // acquire x locks on page cache and log manager
    // let page_cache = Unique::mut_page_cache();
    // let mut log_manager = Unique::mut_log_manager();

    // step 1: start a transaction
    let tx = Transaction::new();
    tx.start().unwrap();

    // step 2: insert a tuple into the table
    insert_row(&table, &tx, key_1);

    // step 3: force flush all pages (from the buffer pool to disk)
    // let page_cache = Unique::mut_page_cache();
    // let mut log_manager = Unique::mut_log_manager();
    // page_cache.flush_all_pages(&mut log_manager);
    Unique::mut_page_cache()
        .flush_all_pages(&mut Unique::mut_log_manager());

    // step 4: insert another tuple into the table
    insert_row(&table, &tx, key_2);

    // step 5: commit the transaction
    tx.manual_commit(&Unique::mut_page_cache()).unwrap();
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
    assert_true(search_key(table, &tx, key_1) == 1, table);
    assert_true(search_key(table, &tx, key_2) == 1, table);

    Unique::mut_log_manager().show_log_contents();

    // step 4: abort the transaction
    if let Err(e) = tx.abort() {
        panic!("abort failed: {}", e);
    }
    // assert_true(tx.abort().is_ok(), table);
}

/// Simulate crash.
/// 1. restart Database
/// 2. run log recovery
fn crash() {
    todo!()
}

// void crash()
//     throws IOException {
//     Database.reset();
//     hf1 = Utility.openHeapFile(2, file1);
//     hf2 = Utility.openHeapFile(2, file2);
//     Database.getLogFile().recover();
// }

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
    assert_true(Unique::log_file().records_count() == 6, &table);

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
    assert_true(search_key(&table, &tx, 1) == 1, &table);
    assert_true(search_key(&table, &tx, 2) == 1, &table);
    assert_true(search_key(&table, &tx, 3) == 0, &table);
    assert_true(search_key(&table, &tx, 4) == 0, &table);
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

    insert_row(&table_1, &tx_1, 4);

    // Unique::log_file().show_log_contents();
    // return;

    tx_1.abort().unwrap();

    // verify the result
    let tx = Transaction::new();
    assert_true(search_key(&table_1, &tx, 1) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, 2) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, 3) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, 4) == 0, &table_1);
    assert_true(search_key(&table_2, &tx, 21) == 1, &table_2);
    assert_true(search_key(&table_2, &tx, 22) == 1, &table_2);
    tx.commit().unwrap();
}

#[test]
fn test_abort_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1.db", 2);
    let table_1 = table_pod_1.rl();
    let table_pod_2 = new_empty_btree_table("table_2.db", 2);
    let table_2 = table_pod_2.rl();

    commit_insert(&table_1, 1, 2);
    abort_insert(&table_1, 4, 5);

    let tx = Transaction::new();
    tx.start().unwrap();
    assert_true(search_key(&table_1, &tx, 1) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, 2) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, 3) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, 4) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, 5) == 0, &table_1);
    tx.commit().unwrap();

    // crash and recover: data should still not be there
    crash();

    let tx = Transaction::new();
    tx.start().unwrap();
    assert_true(search_key(&table_1, &tx, 1) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, 2) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, 3) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, 4) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, 5) == 0, &table_1);
    tx.commit().unwrap();
}
