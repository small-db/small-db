use small_db::{storage::tuple::Cell, transaction::Transaction, utils::HandyRwLock};

use crate::test_utils::{insert_row, new_random_btree_table, search_key, setup, TreeLayout};

/// A transaction should be able to read its own writes, no matter what
/// isolation level we are using.
#[test]
#[cfg(any(
    feature = "read_uncommitted",
    feature = "read_committed",
    feature = "repeatable_read",
    feature = "serializable"
))]
fn test_read_self() {
    setup();

    let table_pod = new_random_btree_table(2, 0, None, 1000, TreeLayout::LastTwoEvenlyDistributed);

    let tx = Transaction::new();

    // The transaction should be able to read its own writes, even before commit.
    {
        let key = 123;

        let table = table_pod.wl();
        insert_row(&table, &tx, key);

        assert!(search_key(&table, &tx, &Cell::Int64(key)) == 1);
    }
}

/// Dirty write happens when a transaction can see and update dirty
/// (uncommitted) data, that has been dirtied by another transaction. This can
/// cause the database to become highly inconsistent. This anomaly is avoided by
/// most of the databases, at even the weakest isolation level.
#[test]
fn test_anomaly_dirty_write() {}

/// A "dirty read" in SQL occurs when a transaction reads data that has been
/// modifiedby another transaction, but not yet committed. In other words, a
/// transaction reads uncommitted data from another transaction, which can lead
/// to incorrect or inconsistent results.
///
/// This anomaly happens in "read uncommitted" isolation level. Isolation levels
/// which have a higher strictness should be able to pass this test.
#[test]
#[cfg(any(
    feature = "read_committed",
    feature = "repeatable_read",
    feature = "serializable"
))]
fn test_anomaly_dirty_read() {
    setup();

    let table_pod = new_random_btree_table(2, 1000, None, 0, TreeLayout::LastTwoEvenlyDistributed);

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

        assert!(search_key(&table_pod.rl(), &read_tx, &Cell::Int64(key)) == 1);
    }
}

/// A Phantom Read occurs when a transaction re-executes a query returning a set
/// of rows that satisfies a search condition and finds that the set of rows has
/// changed due to another transaction.
#[test]
#[cfg(any(
    feature = "read_uncommitted",
    feature = "read_committed",
    feature = "repeatable_read",
    feature = "serializable"
))]
fn test_anomaly_phantom() {
    setup();

    let table_pod = new_random_btree_table(2, 1000, None, 0, TreeLayout::LastTwoEvenlyDistributed);

    let key = 123;
    let init_count = 20;
    {
        let init_tx = Transaction::new();

        let table = table_pod.wl();
        for _ in 0..init_count {
            insert_row(&table, &init_tx, key);
        }
        // we have to drop the table here, since we need to access it in the commit
        // phase
        drop(table);

        init_tx.commit().unwrap();
    }

    // start a read transaction
    let read_tx = Transaction::new();

    // search for the key, the result should be init_count
    {
        assert!(search_key(&table_pod.rl(), &read_tx, &Cell::Int64(key)) == init_count);
    }

    // start a write transaction, insert some new rows, then commit the write
    // transaction
    {
        let write_tx = Transaction::new();

        let table = table_pod.wl();
        for _ in 0..5 {
            insert_row(&table, &write_tx, key);
        }
        // we have to drop the table here, since we need to access it in the commit
        // phase
        drop(table);

        write_tx.commit().unwrap();
    }

    // re-search for the key, the result should stay the same
    {
        assert!(search_key(&table_pod.rl(), &read_tx, &Cell::Int64(key)) == init_count);
    }
}
