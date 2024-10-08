use log::debug;
use small_db::{
    btree::page::BTreePage, storage::tuple::Cell, transaction::Transaction, utils::HandyRwLock,
    BTreeTable, Database,
};

use crate::test_utils::{
    assert_true, crash, insert_row, new_empty_btree_table, new_random_btree_table, search_key,
    setup, TreeLayout,
};

/// Insert two tuples into the table, then commit the transaction. There is a
/// flush action in the middle of the transaction.
///
/// This function doesn't check the corectness of the transaction semantics.
fn commit_insert(table: &BTreeTable, key_1: i64, key_2: i64) {
    // acquire x locks on page cache and log manager
    // let page_cache = Unique::mut_page_cache();
    // let mut log_manager = Unique::mut_log_manager();

    // step 1: start a transaction
    let tx = Transaction::new();

    // step 2: insert a tuple into the table
    insert_row(&table, &tx, key_1);

    // step 3: force flush all pages (from the buffer pool to disk)
    Database::mut_buffer_pool().flush_all_pages(&mut Database::mut_log_manager());

    // step 4: insert another tuple into the table
    insert_row(&table, &tx, key_2);

    // step 5: commit the transaction
    tx.commit().unwrap();
}

/// Insert two tuples into the table, then abort the transaction.
///
/// This function does check the correctness of the transaction semantics.
fn abort_insert(table: &BTreeTable, key_1: i64, key_2: i64) {
    // step 1: start a transaction
    let tx = Transaction::new();

    // step 2: insert two tuples into the table
    insert_row(&table, &tx, key_1);
    insert_row(&table, &tx, key_2);

    // step 3: search for the tuples
    assert_true(search_key(table, &tx, &Cell::Int64(key_1)) == 1, table);
    assert_true(search_key(table, &tx, &Cell::Int64(key_2)) == 1, table);

    // step 4: abort the transaction
    assert!(tx.abort().is_ok());

    // step 5: check if the tuples are gone
    let search_tx = Transaction::new();
    assert!(search_key(table, &search_tx, &Cell::Int64(key_1)) == 0);
    assert!(search_key(table, &search_tx, &Cell::Int64(key_2)) == 0);
    search_tx.commit().unwrap();
}

#[test]
#[cfg(feature = "aries_steal")]
/// Test if the "flush_page" api writes "UPDATE" record to the log.
fn test_flush_page() {
    use crate::test_utils::get_leaf_page;

    setup();

    // Create an empty B+ tree file keyed on the second field of a
    // 2-field tuple.
    let table_rc = new_random_btree_table(2, 0, None, 1, TreeLayout::Naturally);
    let table = table_rc.rl();

    commit_insert(&table, 1, 2);

    Database::mut_log_manager().show_log_contents();

    // Check flush action writes "UPDATE" record to log.
    //
    // There should be 4 records in the log:
    // - tx start - "START"
    // - flush action - "UPDATE" (leaf page)
    // - tx commit - "UPDATE" (leaf page)
    // - tx commit - "COMMIT"
    //
    // We don't use `assert_true` here because it requires a write
    // lock on the log manager.
    assert_eq!(Database::log_manager().records_count(), 4);

    // check that BufferPool.transactionComplete(commit=true) called
    // Page.setBeforeImage().
    let page_rc = get_leaf_page(&table, 0, 0);
    let page = page_rc.rl();
    assert_eq!(
        page.get_page_data(&table.schema),
        page.get_before_image(&table.schema)
    );
}

#[test]
fn test_abort() {
    setup();

    let table_rc = new_random_btree_table(2, 0, None, 1, TreeLayout::Naturally);
    let table = table_rc.rl();

    commit_insert(&table, 1, 2);
    abort_insert(&table, 3, 4);

    let tx = Transaction::new();
    assert_true(search_key(&table, &tx, &Cell::Int64(1)) == 1, &table);
    assert_true(search_key(&table, &tx, &Cell::Int64(2)) == 1, &table);
    assert_true(search_key(&table, &tx, &Cell::Int64(3)) == 0, &table);
    assert_true(search_key(&table, &tx, &Cell::Int64(4)) == 0, &table);
    tx.commit().unwrap();
}

#[test]
fn test_abort_commit_interleaved() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1", 2);
    let table_1 = table_pod_1.rl();
    let table_pod_2 = new_empty_btree_table("table_2", 2);
    let table_2 = table_pod_2.rl();

    commit_insert(&table_1, 1, 2);

    // T1 start, T2 start and commit, T1 abort

    let mut tx_1 = Transaction::new();
    insert_row(&table_1, &mut tx_1, 3);

    let mut tx_2 = Transaction::new();
    insert_row(&table_2, &mut tx_2, 21);
    Database::mut_log_manager().log_checkpoint().unwrap();
    insert_row(&table_2, &mut tx_2, 22);
    tx_2.commit().unwrap();

    insert_row(&table_1, &mut tx_1, 4);

    tx_1.abort().unwrap();

    // verify the result
    let tx = Transaction::new();

    assert_true(search_key(&table_1, &tx, &Cell::Int64(1)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(2)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(3)) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(4)) == 0, &table_1);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(21)) == 1, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(22)) == 1, &table_2);
    tx.commit().unwrap();
}

#[test]
fn test_abort_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1", 2);
    let table_1 = table_pod_1.rl();

    commit_insert(&table_1, 1, 2);
    abort_insert(&table_1, 4, 5);

    /// Check if the table is in the expected state.
    fn check(table: &BTreeTable) {
        let tx = Transaction::new();

        assert_true(search_key(&table, &tx, &Cell::Int64(1)) == 1, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(2)) == 1, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(3)) == 0, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(4)) == 0, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(5)) == 0, &table);
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

    let table_pod_1 = new_empty_btree_table("table_1", 2);
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

        assert_true(search_key(&table, &tx, &Cell::Int64(1)) == 1, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(2)) == 1, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(3)) == 0, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(4)) == 0, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(5)) == 1, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(6)) == 1, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(7)) == 0, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(8)) == 0, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(9)) == 1, &table);
        assert_true(search_key(&table, &tx, &Cell::Int64(10)) == 1, &table);
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

    let table_pod_1 = new_empty_btree_table("table_1", 2);
    let table_1 = table_pod_1.rl();

    // insert, crash, recover: data should still be there

    commit_insert(&table_1, 1, 2);

    crash();

    let tx = Transaction::new();

    assert_true(search_key(&table_1, &tx, &Cell::Int64(1)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(2)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(3)) == 0, &table_1);
    tx.commit().unwrap();
}

#[test]
fn test_open_commit_checkpoint_open_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1", 2);
    let table_1 = table_pod_1.rl();
    let table_pod_2 = new_empty_btree_table("table_2", 2);
    let table_2 = table_pod_2.rl();

    commit_insert(&table_1, 1, 2);

    // T1 inserts but does not commit
    // T2 inserts and commits
    // checkpoint
    // T3 inserts but does not commit
    // crash
    // only T2 data should be there

    let mut tx_1 = Transaction::new();
    insert_row(&table_1, &mut tx_1, 12);

    // defeat NO-STEAL-based abort
    // (since ARIES is a steal/no-force recovery algorithm, we
    // simulate the "steal" scenario here by flushing the buffer
    // pool)
    Database::mut_buffer_pool().flush_all_pages(&mut Database::mut_log_manager());

    insert_row(&table_1, &mut tx_1, 13);
    Database::mut_buffer_pool().flush_all_pages(&mut Database::mut_log_manager());

    insert_row(&table_1, &mut tx_1, 14);

    // T2 commits
    commit_insert(&table_2, 26, 27);

    Database::mut_log_manager().log_checkpoint().unwrap();

    let mut tx_3 = Transaction::new();
    insert_row(&table_2, &mut tx_3, 28);
    // defeat NO-STEAL-based abort
    Database::mut_buffer_pool().flush_all_pages(&mut Database::mut_log_manager());
    insert_row(&table_2, &mut tx_3, 29);

    crash();

    let tx = Transaction::new();

    assert_true(search_key(&table_1, &tx, &Cell::Int64(1)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(2)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(12)) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(13)) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(14)) == 0, &table_1);

    assert_true(search_key(&table_2, &tx, &Cell::Int64(22)) == 0, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(23)) == 0, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(24)) == 0, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(25)) == 0, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(26)) == 1, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(27)) == 1, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(28)) == 0, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(29)) == 0, &table_2);
    tx.commit().unwrap();
}

#[test]
fn test_open_commit_open_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1", 2);
    let table_1 = table_pod_1.rl();
    let table_pod_2 = new_empty_btree_table("table_2", 2);
    let table_2 = table_pod_2.rl();

    commit_insert(&table_1, 1, 2);

    // T1 inserts but does not commit
    // T2 inserts and commits
    // T3 inserts but does not commit
    // crash
    // only T2 data should be there

    // T1 inserts but does not commit (data: 10, 11)
    {
        let mut tx_1 = Transaction::new();
        insert_row(&table_1, &mut tx_1, 10);
        // defeat NO-STEAL-based abort
        Database::mut_buffer_pool().flush_all_pages(&mut Database::mut_log_manager());
        insert_row(&table_1, &mut tx_1, 11);
    }

    // T2 commits (data: 20, 21)
    {
        commit_insert(&table_2, 20, 21);
    }

    // T3 inserts but does not commit (data: 30, 31)
    {
        let mut tx_3 = Transaction::new();
        insert_row(&table_2, &mut tx_3, 30);
        // defeat NO-STEAL-based abort
        Database::mut_buffer_pool().flush_all_pages(&mut Database::mut_log_manager());
        insert_row(&table_2, &mut tx_3, 31);
    }

    Database::mut_log_manager().show_log_contents();

    crash();

    debug!("--- after crash ---");

    Database::mut_log_manager().show_log_contents();

    let tx = Transaction::new();

    // existing data
    assert_true(search_key(&table_1, &tx, &Cell::Int64(1)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(2)) == 1, &table_1);

    // T1 data (should not be there)
    assert_true(search_key(&table_1, &tx, &Cell::Int64(10)) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(11)) == 0, &table_1);

    // T2 data (should be there)
    assert_true(search_key(&table_2, &tx, &Cell::Int64(20)) == 1, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(21)) == 1, &table_2);

    // T3 data (should not be there)
    assert_true(search_key(&table_2, &tx, &Cell::Int64(30)) == 0, &table_2);
    assert_true(search_key(&table_2, &tx, &Cell::Int64(31)) == 0, &table_2);

    tx.commit().unwrap();
}

#[test]
fn test_open_crash() {
    setup();

    let table_pod_1 = new_empty_btree_table("table_1", 2);
    let table_1 = table_pod_1.rl();

    commit_insert(&table_1, 1, 2);

    // step 1: write_tx inserts some data but does not commit
    let mut write_tx = Transaction::new();
    insert_row(&table_1, &mut write_tx, 8);
    Database::mut_buffer_pool().flush_all_pages(&mut Database::mut_log_manager());
    insert_row(&table_1, &mut write_tx, 9);

    Database::mut_log_manager().show_log_contents();

    // step 2: crash
    crash();

    // result: the data it inserted should not be there
    let tx = Transaction::new();
    assert_true(search_key(&table_1, &tx, &Cell::Int64(1)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(2)) == 1, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(8)) == 0, &table_1);
    assert_true(search_key(&table_1, &tx, &Cell::Int64(9)) == 0, &table_1);
    tx.commit().unwrap();
}

/// Create a table, test if it still exists after a crash.
#[test]
fn test_new_table() {
    setup();

    let table_name = "table_abc";

    let _ = new_empty_btree_table(table_name, 2);

    crash();

    assert!(Database::catalog().search_table(table_name).is_some());
}
