use log::debug;
use small_db::{storage::tuple::Cell, transaction::Transaction, utils::HandyRwLock};

use crate::test_utils::{insert_row, new_random_btree_table, search_key, setup, TreeLayout};

#[test]
/// Test the "dirty read" anomaly.
///
/// This anomaly happens in "Read Uncommitted" isolation level. Isolation levels which have
/// a higher strictness should be able to pass this test.
#[cfg(feature = "benchmark")]
fn test_anomaly_dirty_read() {
    setup();

    let table_pod = new_random_btree_table(2, 0, None, 0, TreeLayout::LastTwoEvenlyDistributed);

    // start a write transaction
    let write_tx = Transaction::new();

    // start a read transaction
    let read_tx = Transaction::new();

    let key = 123;

    // write something, the read transaction should not be able to see it
    {
        let table = table_pod.wl();
        insert_row(&table, &write_tx, key);

        assert!(search_key(&table, &read_tx, &Cell::Int64(key)) == 0);
    }

    // commit, then the read transaction should be able to see the it
    {
        write_tx.commit().unwrap();

        debug!(
            "serach result: {}",
            search_key(&table_pod.rl(), &read_tx, &Cell::Int64(key))
        );

        assert!(search_key(&table_pod.rl(), &read_tx, &Cell::Int64(key)) == 1);
    }
}

#[test]
/// A transaction should be able to read its own writes.
fn test_read_self() {
    setup();

    let table_pod = new_random_btree_table(2, 0, None, 0, TreeLayout::LastTwoEvenlyDistributed);

    let tx = Transaction::new();

    // The transaction should be able to read its own writes, even before commit.
    {
        let key = 123;

        let table = table_pod.wl();
        insert_row(&table, &tx, key);

        assert!(search_key(&table, &tx, &Cell::Int64(key)) == 1);
    }
}
