use small_db::{
    btree::page::BTreePage,
    storage::tuple::{Cell, Tuple},
    transaction::Transaction,
    utils::HandyRwLock,
    BTreeTable, Database,
};

use crate::test_utils::{
    assert_true, get_leaf_page, new_empty_btree_table,
    new_random_btree_table, search_key, setup, TreeLayout,
};

fn insert_row(table: &BTreeTable, tx: &Transaction, key: i32) {
    let tuple = Tuple::new_int_tuples(key, 2);
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
    Database::mut_page_cache()
        .flush_all_pages(&mut Database::mut_log_manager());

    // step 4: insert another tuple into the table
    insert_row(&table, &tx, key_2);

    // step 5: commit the transaction
    tx.manual_commit(&Database::mut_page_cache()).unwrap();
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
    assert_true(
        search_key(table, &tx, &Cell::Int32(key_1)) == 1,
        table,
    );
    assert_true(
        search_key(table, &tx, &Cell::Int32(key_2)) == 1,
        table,
    );

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
    Database::reset();

    Database::mut_log_manager().recover().unwrap();
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
    assert_true(Database::log_file().records_count() == 6, &table);

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
    assert_true(
        search_key(&table, &tx, &Cell::Int32(1)) == 1,
        &table,
    );
    assert_true(
        search_key(&table, &tx, &Cell::Int32(2)) == 1,
        &table,
    );
    assert_true(
        search_key(&table, &tx, &Cell::Int32(3)) == 0,
        &table,
    );
    assert_true(
        search_key(&table, &tx, &Cell::Int32(4)) == 0,
        &table,
    );
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
    Database::mut_log_manager().log_checkpoint().unwrap();
    insert_row(&table_2, &tx_2, 22);
    tx_2.commit().unwrap();

    insert_row(&table_1, &tx_1, 4);

    // Unique::log_file().show_log_contents();
    // return;

    tx_1.abort().unwrap();

    // verify the result
    let tx = Transaction::new();
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(1)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(2)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(3)) == 0,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(4)) == 0,
        &table_1,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(21)) == 1,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(22)) == 1,
        &table_2,
    );
    tx.commit().unwrap();
}

#[test]
fn test_abort_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1.db", 2);
    let table_1 = table_pod_1.rl();

    commit_insert(&table_1, 1, 2);
    abort_insert(&table_1, 4, 5);

    fn check(table: &BTreeTable) {
        let tx = Transaction::new();
        tx.start().unwrap();
        assert_true(
            search_key(&table, &tx, &Cell::Int32(1)) == 1,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(2)) == 1,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(3)) == 0,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(4)) == 0,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(5)) == 0,
            &table,
        );
        tx.commit().unwrap();
    }

    check(&table_1);

    // crash and recover: data should still not be there
    crash();

    check(&table_1);
}

#[test]
fn test_commit_abort_commit_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1.db", 2);
    let table_1 = table_pod_1.rl();

    commit_insert(&table_1, 1, 2);

    // T1 inserts and commits
    // T2 inserts but aborts
    // T3 inserts and commits
    // only T1 and T3 data should be there

    commit_insert(&table_1, 5, 6);
    abort_insert(&table_1, 7, 8);
    commit_insert(&table_1, 9, 10);

    fn check(table: &BTreeTable) {
        let tx = Transaction::new();
        tx.start().unwrap();
        assert_true(
            search_key(&table, &tx, &Cell::Int32(1)) == 1,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(2)) == 1,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(3)) == 0,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(4)) == 0,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(5)) == 1,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(6)) == 1,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(7)) == 0,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(8)) == 0,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(9)) == 1,
            &table,
        );
        assert_true(
            search_key(&table, &tx, &Cell::Int32(10)) == 1,
            &table,
        );
        tx.commit().unwrap();
    }

    check(&table_1);

    // crash: should not change visible data
    crash();

    check(&table_1);
}

#[test]
fn test_commit_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1.db", 2);
    let table_1 = table_pod_1.rl();

    // insert, crash, recover: data should still be there

    commit_insert(&table_1, 1, 2);

    crash();

    let tx = Transaction::new();
    tx.start().unwrap();
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(1)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(2)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(3)) == 0,
        &table_1,
    );
    tx.commit().unwrap();
}

#[test]
/// Skip this test since it's designed to test the heap-file
/// implementation.
fn test_flush_all() {}

#[test]
fn test_open_commit_checkpoint_open_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1.db", 2);
    let table_1 = table_pod_1.rl();
    let table_pod_2 = new_empty_btree_table("table_2.db", 2);
    let table_2 = table_pod_2.rl();

    commit_insert(&table_1, 1, 2);

    // T1 inserts but does not commit
    // T2 inserts and commits
    // checkpoint
    // T3 inserts but does not commit
    // crash
    // only T2 data should be there

    let t1 = Transaction::new();
    t1.start().unwrap();
    insert_row(&table_1, &t1, 12);

    // defeat NO-STEAL-based abort
    // (since ARIES is a steal/no-force recovery algorithm, we
    // simulate the "steal" scenario here by flushing the buffer
    // pool)
    Database::mut_page_cache()
        .flush_all_pages(&mut Database::mut_log_manager());

    insert_row(&table_1, &t1, 13);
    Database::mut_page_cache()
        .flush_all_pages(&mut Database::mut_log_manager());

    insert_row(&table_1, &t1, 14);

    // T2 commits
    commit_insert(&table_2, 26, 27);

    Database::mut_log_manager().log_checkpoint().unwrap();

    let tx_3 = Transaction::new();
    tx_3.start().unwrap();
    insert_row(&table_2, &tx_3, 28);
    // defeat NO-STEAL-based abort
    Database::mut_page_cache()
        .flush_all_pages(&mut Database::mut_log_manager());
    insert_row(&table_2, &tx_3, 29);

    crash();

    let tx = Transaction::new();
    tx.start().unwrap();
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(1)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(2)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(12)) == 0,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(13)) == 0,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(14)) == 0,
        &table_1,
    );

    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(22)) == 0,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(23)) == 0,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(24)) == 0,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(25)) == 0,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(26)) == 1,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(27)) == 1,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(28)) == 0,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(29)) == 0,
        &table_2,
    );
    tx.commit().unwrap();
}

#[test]
fn test_open_commit_open_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1.db", 2);
    let table_1 = table_pod_1.rl();
    let table_pod_2 = new_empty_btree_table("table_2.db", 2);
    let table_2 = table_pod_2.rl();

    commit_insert(&table_1, 1, 2);

    // T1 inserts but does not commit
    // T2 inserts and commits
    // T3 inserts but does not commit
    // crash
    // only T2 data should be there

    let tx_1 = Transaction::new();
    tx_1.start().unwrap();
    insert_row(&table_1, &tx_1, 10);
    // defeat NO-STEAL-based abort
    Database::mut_page_cache()
        .flush_all_pages(&mut Database::mut_log_manager());
    insert_row(&table_1, &tx_1, 11);

    // T2 commits
    commit_insert(&table_2, 22, 23);

    let tx_3 = Transaction::new();
    tx_3.start().unwrap();
    insert_row(&table_2, &tx_3, 24);
    // defeat NO-STEAL-based abort
    Database::mut_page_cache()
        .flush_all_pages(&mut Database::mut_log_manager());
    insert_row(&table_2, &tx_3, 25);

    crash();

    let tx = Transaction::new();
    tx.start().unwrap();
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(1)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(2)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(10)) == 0,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(11)) == 0,
        &table_1,
    );

    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(22)) == 1,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(23)) == 1,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(24)) == 0,
        &table_2,
    );
    assert_true(
        search_key(&table_2, &tx, &Cell::Int32(25)) == 0,
        &table_2,
    );
    tx.commit().unwrap();
}

#[test]
fn test_open_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1.db", 2);
    let table_1 = table_pod_1.rl();

    commit_insert(&table_1, 1, 2);

    // T1 inserts but does not commit
    // crash
    // no data should not be there

    let tx_1 = Transaction::new();
    tx_1.start().unwrap();
    insert_row(&table_1, &tx_1, 8);
    // something to UNDO (what?)
    Database::mut_page_cache()
        .flush_all_pages(&mut Database::mut_log_manager());
    insert_row(&table_1, &tx_1, 9);

    crash();

    let tx = Transaction::new();
    tx.start().unwrap();
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(1)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(2)) == 1,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(8)) == 0,
        &table_1,
    );
    assert_true(
        search_key(&table_1, &tx, &Cell::Int32(9)) == 0,
        &table_1,
    );
    tx.commit().unwrap();
}

// @Test public void TestOpenCrash()
// throws IOException, DbException, TransactionAbortedException {
// setup();
// doInsert(hf1, 1, 2);

// // *** Test:
// // insert but no commit
// // crash
// // data should not be there

// Transaction t = new Transaction();
// t.start();
// insertRow(hf1, t, 8);
// Database.getBufferPool().flushAllPages(); // XXX something to UNDO
// insertRow(hf1, t, 9);

// crash();

// t = new Transaction();
// t.start();
// look(hf1, t, 1, true);
// look(hf1, t, 8, false);
// look(hf1, t, 9, false);
// t.commit();
// }
